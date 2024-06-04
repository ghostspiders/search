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

package org.server.search.gateway;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.*;
import org.server.search.cluster.metadata.IndexMetaData;
import org.server.search.cluster.metadata.MetaData;
import org.server.search.cluster.metadata.MetaDataService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.concurrent.DynamicExecutors;
import org.server.search.util.settings.Settings;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.server.search.cluster.ClusterState.*;
import static org.server.search.cluster.metadata.MetaData.*;
import static org.server.search.util.TimeValue.*;


public class GatewayService extends AbstractComponent implements ClusterStateListener, LifecycleComponent<GatewayService> {

    // 生命周期管理对象，用于管理GatewayService的启动、停止等状态
    private final Lifecycle lifecycle = new Lifecycle();

    // Gateway接口的实现，负责处理与索引恢复和快照相关的操作
    private final Gateway gateway;

    // 线程池，用于执行异步任务和并发处理
    private final ThreadPool threadPool;

    // 用于执行GatewayService相关任务的ExecutorService
    private volatile ExecutorService executor;

    // 集群服务，用于获取和更新集群状态
    private final ClusterService clusterService;

    // 元数据服务，用于操作和管理索引的元数据
    private final MetaDataService metaDataService;

    // 一个原子布尔值，用于标识是否完成了首次从主节点的读取操作
    private final AtomicBoolean firstMasterRead = new AtomicBoolean();

    @Inject public GatewayService(Settings settings, Gateway gateway, ClusterService clusterService,
                                  ThreadPool threadPool, MetaDataService metaDataService) {
        super(settings);
        this.gateway = gateway;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.metaDataService = metaDataService;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public GatewayService start() throws Exception {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        gateway.start();
        this.executor = Executors.newSingleThreadExecutor(DynamicExecutors.daemonThreadFactory(settings, "gateway"));
        clusterService.add(this);
        return this;
    }

    @Override public GatewayService stop() throws SearchException, InterruptedException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        clusterService.remove(this);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        gateway.stop();
        return this;
    }

    public void close() throws InterruptedException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        gateway.close();
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        // 检查GatewayService是否已启动，以及当前节点是否是主节点
        if (lifecycle.started() && event.localNodeMaster()) {
            // 如果是首次选举出主节点，并且firstMasterRead标志位从未被设置过，则从网关读取数据
            if (event.firstMaster() && firstMasterRead.compareAndSet(false, true)) {
                readFromGateway(); // 从持久化存储读取集群状态和元数据信息
            } else {
                // 如果当前节点是主节点，并且这已经不是首次主节点选举，则将集群状态写入到网关
                writeToGateway(event); // 将集群状态持久化到共享存储，以便其他节点可以恢复
            }
        }
    }

    private void writeToGateway(final ClusterChangedEvent event) {
        // 检查元数据是否发生了变化，如果没有变化，则不需要写入网关
        if (!event.metaDataChanged()) {
            return;
        }
        // 在executor线程池中执行写入操作
        executor.execute(new Runnable() {
            @Override
            public void run() {
                // 如果调试模式开启，则记录写入网关的日志
                logger.debug("Writing to gateway");
                try {
                    // 调用gateway接口的write方法，传入当前集群状态的元数据进行持久化
                    gateway.write(event.state().metaData());
                    // 这里有一个TODO注释，表示需要记录失败的情况，可能需要添加重试调度机制
                } catch (Exception e) {
                    // 如果写入网关失败，记录错误日志
                    logger.error("Failed to write to gateway", e);
                }
            }
        });
    }
    private void readFromGateway() {
        // 如果当前节点是集群中的第一个主节点，那么读取网关上的状态并创建索引
        logger.debug("First master in the cluster, reading state from gateway");
        // 在executor线程池中执行读取操作
        executor.execute(new Runnable() {
            @Override
            public void run() {
                MetaData metaData;
                try {
                    // 调用gateway接口的read方法，从持久化存储中读取元数据
                    metaData = gateway.read();
                } catch (Exception e) {
                    // 如果读取过程中发生异常，记录错误日志并返回
                    logger.error("Failed to read from gateway", e);
                    return;
                }
                // 如果读取的元数据为null，记录一条日志信息并返回
                if (metaData == null) {
                    logger.debug("No state read from gateway");
                    return;
                }
                // 将读取到的元数据赋值给局部变量fMetaData，用于后续操作
                final MetaData fMetaData = metaData;
                // 提交一个集群状态更新任务，用于更新集群状态并创建索引
                clusterService.submitStateUpdateTask("gateway (recovered meta-data)", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        // 创建一个新的MetaData.Builder，用于构建更新后的元数据
                        MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                                .metaData(currentState.metaData()).maxNumberOfShardsPerNode(fMetaData.maxNumberOfShardsPerNode());
                        // 遍历读取到的索引元数据
                        for (final IndexMetaData indexMetaData : fMetaData) {
                            // 在线程池中为每个索引元数据执行创建索引的任务
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        // 调用metaDataService的createIndex方法创建索引
                                        metaDataService.createIndex(indexMetaData.index(), indexMetaData.settings(), timeValueMillis(10));
                                    } catch (Exception e) {
                                        // 如果创建索引过程中发生异常，记录错误日志
                                        logger.error("Failed to create index [" + indexMetaData.index() + "]", e);
                                    }
                                }
                            });
                        }
                        // 构建并返回新的集群状态
                        return newClusterStateBuilder().state(currentState).metaData(metaDataBuilder).build();
                    }
                });
            }
        });
    }
}
