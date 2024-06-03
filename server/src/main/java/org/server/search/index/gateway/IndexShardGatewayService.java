/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.server.search.index.gateway;

import com.google.inject.Inject;
import org.apache.lucene.index.IndexCommit;
import org.server.search.SearchIllegalStateException;
import org.server.search.index.deletionpolicy.SnapshotIndexCommit;
import org.server.search.index.engine.Engine;
import org.server.search.index.engine.EngineException;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.*;
import org.server.search.index.store.Store;
import org.server.search.index.translog.Translog;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.StopWatch;
import org.server.search.util.TimeValue;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexShardGatewayService extends AbstractIndexShardComponent {

    /**
     * 标记关闭分片时是否应该创建快照。
     */
    private final boolean snapshotOnClose;

    /**
     * 用于执行与网关服务相关的异步任务的线程池。
     */
    private final ThreadPool threadPool;

    /**
     * 一个引用，指向这个网关服务所关联的内部索引分片。
     */
    private final InternalIndexShard indexShard;

    /**
     * 负责执行恢复和快照等操作的分片网关组件。
     */
    private final IndexShardGateway shardGateway;

    /**
     * 索引分片的存储组件，负责数据的持久化存储。
     */
    private final Store store;

    /**
     * 最后索引文档的版本。
     */
    private volatile long lastIndexVersion;

    /**
     * 记录的最后一个事务日志（translog）ID。
     */
    private volatile long lastTranslogId = -1;

    /**
     * 最后一个事务日志的大小。
     */
    private volatile int lastTranslogSize;

    /**
     * 原子布尔标志，指示分片是否已恢复。
     */
    private final AtomicBoolean recovered = new AtomicBoolean();

    /**
     * 应该采取快照的时间间隔。
     */
    private final TimeValue snapshotInterval;

    /**
     * 用于管理快照任务执行时间的计划任务。
     */
    private volatile ScheduledFuture snapshotScheduleFuture;

    @Inject public IndexShardGatewayService(ShardId shardId, @IndexSettings Settings indexSettings,
                                            ThreadPool threadPool, IndexShard indexShard, IndexShardGateway shardGateway,
                                            Store store) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.indexShard = (InternalIndexShard) indexShard;
        this.shardGateway = shardGateway;
        this.store = store;

        this.snapshotOnClose = componentSettings.getAsBoolean("snapshotOnClose", true);
        this.snapshotInterval = componentSettings.getAsTime("snapshotInterval", TimeValue.timeValueSeconds(10));
    }

    /**
     * 应在分片路由状态发生变化时调用（注意，在分片上设置状态后）。
     * 当分片的路由状态发生变化时，可能会触发一些操作，比如检查是否需要创建快照。
     * 这个方法会根据当前的状态和配置来决定是否安排快照任务。
     */
    public void routingStateChanged() {
        scheduleSnapshotIfNeeded();
    }

    /**
     * 从网关恢复分片的状态。
     * 这是一个同步方法，确保在恢复过程中不会发生并发问题。
     * @throws IndexShardGatewayRecoveryException 网关恢复时发生异常
     * @throws IgnoreGatewayRecoveryException 忽略网关恢复，如果分片已经恢复
     * @throws IOException IO操作发生异常
     */
    public synchronized void recover() throws IndexShardGatewayRecoveryException, IgnoreGatewayRecoveryException, IOException {
        if (recovered.compareAndSet(false, true)) { // 如果分片尚未恢复，将其标记为恢复中
            if (!indexShard.routingEntry().primary()) { // 如果分片不是主分片，则抛出异常
                throw new SearchIllegalStateException("Trying to recover when the shard is in backup state");
            }
            // 清空存储，准备恢复数据
            try {
                store.deleteContent();
            } catch (IOException e) {
                logger.debug("Failed to delete store before recovery from gateway", e);
            }
            indexShard.recovering(); // 标记分片为恢复状态
            logger.debug("Starting recovery from {}", shardGateway); // 日志记录开始恢复
            StopWatch stopWatch = new StopWatch().start(); // 开始计时恢复过程
            RecoveryStatus recoveryStatus = shardGateway.recover(); // 执行网关恢复操作

            // 更新最新状态
            indexShard.snapshot(new Engine.SnapshotHandler() {
                @Override public void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                    lastIndexVersion = snapshotIndexCommit.getGeneration(); // 设置最后索引版本
                    lastTranslogId = translogSnapshot.translogId(); // 设置最后事务日志ID
                    lastTranslogSize = translogSnapshot.size(); // 设置最后事务日志大小
                }
            });

            // 如果网关尚未启动分片，则启动分片
            if (indexShard.state() != IndexShardState.STARTED) {
                indexShard.start();
            }
            stopWatch.stop(); // 停止计时
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Recovery completed from ").append(shardGateway).append(", took [").append(stopWatch.totalTime()).append("]\n");
                sb.append("    Index    : numberOfFiles      [").append(recoveryStatus.index().numberOfFiles()).append("] with totalSize [").append(recoveryStatus.index().totalSize()).append("]\n");
                sb.append("    Translog : numberOfOperations [").append(recoveryStatus.translog().numberOfOperations()).append("] with totalSize [").append(recoveryStatus.translog().totalSize()).append("]");
                logger.debug(sb.toString()); // 日志记录恢复详细信息
            }
            // 刷新分片
            indexShard.refresh(false);
            scheduleSnapshotIfNeeded(); // 安排必要时的快照
        } else {
            // 如果分片已经恢复，则抛出忽略网关恢复异常
            throw new IgnoreGatewayRecoveryException(shardId, "Already recovered");
        }
    }

    /**
     * 对指定的分片进行快照操作并存储到网关。
     * 这是一个同步方法，确保在进行快照时不会发生并发问题。
     * @throws IndexShardGatewaySnapshotFailedException 如果快照失败时抛出异常
     */
    public synchronized void snapshot() throws IndexShardGatewaySnapshotFailedException {
        if (!indexShard.routingEntry().primary()) {
            // 如果当前分片不是主分片，则直接返回或抛出异常
            return;
            // throw new IndexShardGatewaySnapshotNotAllowedException(shardId, "Snapshot not allowed on non primary shard");
        }
        if (indexShard.routingEntry().relocating()) {
            // 如果分片正在迁移过程中，则不进行快照，以避免产生冲突
            return;
        }
        indexShard.snapshot(new Engine.SnapshotHandler() {
            @Override
            public void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) throws EngineException {
                // 如果当前快照与上次快照的索引版本、事务日志ID或大小不一致
                if (lastIndexVersion != snapshotIndexCommit.getGeneration() ||
                        lastTranslogId != translogSnapshot.translogId() ||
                        lastTranslogSize != translogSnapshot.size()) {

                    shardGateway.snapshot(snapshotIndexCommit, translogSnapshot);

                    // 更新快照后的索引版本、事务日志ID和大小
                    lastIndexVersion = snapshotIndexCommit.getGeneration();
                    lastTranslogId = translogSnapshot.translogId();
                    lastTranslogSize = translogSnapshot.size();
                }
            }
        });
    }

    public void close() {
        if (snapshotScheduleFuture != null) {
            snapshotScheduleFuture.cancel(true);
            snapshotScheduleFuture = null;
        }
        if (snapshotOnClose) {
            logger.debug("Snapshotting on close ...");
            snapshot();
        }
        shardGateway.close();
    }

    /**
     * 根据需要安排快照任务。
     * 这是一个私有同步方法，确保在安排快照任务时不会发生并发问题。
     */
    private synchronized void scheduleSnapshotIfNeeded() {
        if (!shardGateway.requiresSnapshotScheduling()) {
            // 如果分片网关不需要安排快照，则直接返回
            return;
        }
        if (!indexShard.routingEntry().primary()) {
            // 我们只在主分片上进行快照操作
            return;
        }
        if (!indexShard.routingEntry().started()) {
            // 我们只在集群假定我们已经启动后再安排快照
            return;
        }
        if (snapshotScheduleFuture != null) {
            // 如果我们已经安排了快照任务，则忽略此次调用
            return;
        }
        if (snapshotInterval.millis() != -1) {
            // 如果设置了快照时间间隔，则需要安排快照任务
            if (logger.isDebugEnabled()) {
                logger.debug("Scheduling snapshot every [{}]", snapshotInterval);
            }
            // 使用线程池安排周期性的快照任务
            snapshotScheduleFuture = threadPool.scheduleWithFixedDelay(new SnapshotRunnable(), snapshotInterval);
        }
    }

    private class SnapshotRunnable implements Runnable {
        @Override public void run() {
            try {
                snapshot();
            } catch (Exception e) {
                logger.warn("Failed to snapshot", e);
            }
        }
    }
}
