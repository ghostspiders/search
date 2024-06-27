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

package org.server.search.cluster;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.node.Nodes;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.TimeValue;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.settings.Settings;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.*;
import static org.server.search.cluster.ClusterState.*;
import static org.server.search.util.TimeValue.*;
import static org.server.search.util.concurrent.DynamicExecutors.*;


public class DefaultClusterService extends AbstractComponent implements ClusterService {

    // 生命周期管理对象，用于管理服务的启动、停止等状态
    private final Lifecycle lifecycle = new Lifecycle();

    // 超时时间间隔，用于设置各种超时相关的操作
    private final TimeValue timeoutInterval;

    // 线程池，用于执行异步任务和并发处理
    private final ThreadPool threadPool;

    // 服务发现服务，用于处理节点间的发现和通信
    private final DiscoveryService discoveryService;

    // 传输服务，负责节点间的数据传输
    private final TransportService transportService;

    // 用于执行集群状态更新任务的ExecutorService，可能会根据需要进行更新
    private volatile ExecutorService updateTasksExecutor;

    // 集群状态监听器列表，用于在集群状态发生变化时进行通知
    private final List<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<ClusterStateListener>();

    // 集群状态超时监听器列表，用于处理集群状态超时事件
    private final List<TimeoutHolder> clusterStateTimeoutListeners = new CopyOnWriteArrayList<TimeoutHolder>();

    // 计划执行的任务，例如定期检查集群状态的变更
    private volatile ScheduledFuture scheduledFuture;

    // 当前的集群状态，初始时通过newClusterStateBuilder().build()创建
    private volatile ClusterState clusterState = newClusterStateBuilder().build();

    @Inject public DefaultClusterService(Settings settings, DiscoveryService discoveryService, TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.transportService = transportService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;

        this.timeoutInterval = componentSettings.getAsTime("timeoutInterval", timeValueMillis(500));
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public ClusterService start() throws SearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        this.updateTasksExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "clusterService#updateTask"));
        scheduledFuture = threadPool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                long timestamp = System.currentTimeMillis();
                for (final TimeoutHolder holder : clusterStateTimeoutListeners) {
                    if ((timestamp - holder.timestamp) > holder.timeout.millis()) {
                        clusterStateTimeoutListeners.remove(holder);
                        DefaultClusterService.this.threadPool.execute(new Runnable() {
                            @Override public void run() {
                                holder.listener.onTimeout(holder.timeout);
                            }
                        });
                    }
                }
            }
        }, timeoutInterval);
        return this;
    }

    @Override public ClusterService stop() throws SearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        scheduledFuture.cancel(false);
        for (TimeoutHolder holder : clusterStateTimeoutListeners) {
            holder.listener.onTimeout(holder.timeout);
        }
        updateTasksExecutor.shutdown();
        try {
            updateTasksExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        return this;
    }

    @Override public void close() throws SearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    public ClusterState state() {
        return this.clusterState;
    }

    public void add(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    public void remove(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    public void add(TimeValue timeout, TimeoutClusterStateListener listener) {
        clusterStateTimeoutListeners.add(new TimeoutHolder(listener, System.currentTimeMillis(), timeout));
    }

    public void remove(TimeoutClusterStateListener listener) {
        clusterStateTimeoutListeners.remove(new TimeoutHolder(listener, -1, null));
    }

    // 提交一个集群状态更新任务
    public void submitStateUpdateTask(final String source, final ClusterStateUpdateTask updateTask) {
        // 检查服务是否已启动，如果没有，则直接返回
        if (!lifecycle.started()) {
            return;
        }
        // 在updateTasksExecutor线程池中执行更新任务
        updateTasksExecutor.execute(new Runnable() {
            @Override public void run() {
                // 再次检查服务是否已启动，如果没有，则直接返回
                if (!lifecycle.started()) {
                    return;
                }
                // 记录旧的集群状态
                ClusterState previousClusterState = clusterState;

                // 执行更新任务，获取新的集群状态
                clusterState = updateTask.execute(previousClusterState);

                // 如果集群状态发生了变化
                if (previousClusterState != clusterState) {
                    // 如果是主节点，控制版本号的递增
                    if (clusterState.nodes().localNodeMaster()) {
                        clusterState = newClusterStateBuilder().state(clusterState).incrementVersion().build();
                    }

                    // 如果启用了调试日志，记录集群状态更新信息
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cluster state updated, version [{}], source [{}]", clusterState.version(), source);
                    }

                    // 如果启用了跟踪日志，打印详细的集群状态信息
                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder("Cluster State:\n");
                        sb.append(clusterState.nodes().prettyPrint());
                        sb.append(clusterState.routingTable().prettyPrint());
                        sb.append(clusterState.routingNodes().prettyPrint());
                        logger.trace(sb.toString());
                    }

                    // 创建集群状态变更事件
                    ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, clusterState, previousClusterState, discoveryService.firstMaster());

                    // 检查是否有节点变化，并在有变化时记录日志
                    final Nodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                    if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                        String summary = nodesDelta.shortSummary();
                        if (summary.length() > 0) {
                            logger.info(summary);
                        }
                    }

                    // 在线程池中执行，处理节点添加逻辑
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            transportService.nodesAdded(nodesDelta.addedNodes());
                        }
                    });

                    // 通知所有集群状态超时监听器集群状态已变更
                    for (TimeoutHolder timeoutHolder : clusterStateTimeoutListeners) {
                        timeoutHolder.listener.clusterChanged(clusterChangedEvent);
                    }

                    // 通知所有集群状态监听器集群状态已变更
                    for (ClusterStateListener listener : clusterStateListeners) {
                        listener.clusterChanged(clusterChangedEvent);
                    }

                    // 在线程池中执行，处理节点移除逻辑
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            transportService.nodesRemoved(nodesDelta.removedNodes());
                        }
                    });

                    // 如果是主节点，发布新的集群状态到所有节点
                    if (clusterState.nodes().localNodeMaster()) {
                        discoveryService.publish(clusterState);
                    }

                    // 如果更新任务是ProcessedClusterStateUpdateTask的实例，执行额外的处理
                    if (updateTask instanceof ProcessedClusterStateUpdateTask) {
                        ((ProcessedClusterStateUpdateTask) updateTask).clusterStateProcessed(clusterState);
                    }
                }
            }
        });
    }

    private static class TimeoutHolder {
        final TimeoutClusterStateListener listener;
        final long timestamp;
        final TimeValue timeout;

        private TimeoutHolder(TimeoutClusterStateListener listener, long timestamp, TimeValue timeout) {
            this.listener = listener;
            this.timestamp = timestamp;
            this.timeout = timeout;
        }

        @Override public int hashCode() {
            return listener.hashCode();
        }

        @Override public boolean equals(Object obj) {
            return ((TimeoutHolder) obj).listener == listener;
        }
    }
}