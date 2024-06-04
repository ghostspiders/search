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

package org.server.search.action.support.replication;

import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.action.ActionResponse;
import org.server.search.action.PrimaryNotStartedActionException;
import org.server.search.action.ShardOperationFailedException;
import org.server.search.action.support.BaseAction;
import org.server.search.cluster.ClusterChangedEvent;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.TimeoutClusterStateListener;
import org.server.search.cluster.action.shard.ShardStateAction;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.index.IndexShardMissingException;
import org.server.search.index.shard.IllegalIndexShardStateException;
import org.server.search.index.shard.IndexShard;
import org.server.search.index.shard.IndexShardNotStartedException;
import org.server.search.indices.IndexMissingException;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.*;
import org.server.search.util.TimeValue;
import org.server.search.util.io.Streamable;
import org.server.search.util.io.VoidStreamable;
import org.server.search.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

 
public abstract class TransportShardReplicationOperationAction<Request extends ShardReplicationOperationRequest, Response extends ActionResponse> extends BaseAction<Request, Response> {

    //用于提供节点间通信服务
    protected final TransportService transportService;

    //用于集群状态管理
    protected final ClusterService clusterService;

    //用于管理索引服务
    protected final IndicesService indicesService;

    //提供线程池功能，用于执行异步任务和并发处理
    protected final ThreadPool threadPool;

    //用于处理分片状态变更
    protected final ShardStateAction shardStateAction;

    protected TransportShardReplicationOperationAction(Settings settings, TransportService transportService,
                                                       ClusterService clusterService, IndicesService indicesService,
                                                       ThreadPool threadPool, ShardStateAction shardStateAction) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.shardStateAction = shardStateAction;

        transportService.registerHandler(transportAction(), new OperationTransportHandler());
        transportService.registerHandler(transportBackupAction(), new BackupOperationTransportHandler());
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        // 创建一个新的异步分片操作动作，并启动它
        new AsyncShardOperationAction(request, listener).start();
    }

    //用于创建一个新的请求实例
    protected abstract Request newRequestInstance();

    //用于创建一个新的响应实例
    protected abstract Response newResponseInstance();

    //用于返回传输层操作的名称
    protected abstract String transportAction();

    // 用于在主分片上执行操作并返回响应
    protected abstract Response shardOperationOnPrimary(ShardOperationRequest shardRequest) throws IOException;

    // 用于在备份分片上执行操作
    protected abstract void shardOperationOnBackup(ShardOperationRequest shardRequest) throws IOException;

    //用于返回涉及的分片迭代器
    protected abstract ShardsIterator shards(Request request) throws SearchException;

    /**
     * 是否在备份分片上执行操作。默认值为false，意味着操作也会在备份分片上执行。
     * @return 如果为true，则操作不会在备份分片上执行；如果为false，则操作会在备份分片上执行。
     */
    protected boolean ignoreBackups() {
        return false; // 默认返回false，表示操作会在备份分片上执行
    }

    // 返回传输层备份操作的名称，通常是基本传输操作名称后加上"/backup"
    private String transportBackupAction() {
        return transportAction() + "/backup";
    }

    // 根据分片操作请求获取对应的IndexShard对象
    protected IndexShard indexShard(ShardOperationRequest shardRequest) {
        return indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
    }

    private class OperationTransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequestInstance();
        }

        @Override
        public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // 没有必要使用线程化的监听器，因为我们只是简单地发送一个响应
            request.listenerThreaded(false);
            // 如果我们有一个本地操作，那么在线程上执行它，因为我们不生成线程
            request.operationThreaded(true);

            // 执行请求，并提供一个ActionListener来处理响应或失败
            execute(request, new ActionListener<Response>() {
                @Override
                public void onResponse(Response result) {
                    try {
                        // 发送响应到传输通道
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        // 如果发送响应时发生异常，则调用onFailure处理
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        // 发送错误响应到传输通道
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        // 如果发送错误响应失败，则记录警告日志
                        logger.warn("Failed to send response for " + transportAction(), e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class BackupOperationTransportHandler extends BaseTransportRequestHandler<ShardOperationRequest> {

        @Override public ShardOperationRequest newInstance() {
            return new ShardOperationRequest();
        }

        @Override public void messageReceived(ShardOperationRequest request, TransportChannel channel) throws Exception {
            shardOperationOnBackup(request);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    protected class ShardOperationRequest implements Streamable {

        public int shardId;

        public Request request;

        public ShardOperationRequest() {
        }

        public ShardOperationRequest(int shardId, Request request) {
            this.shardId = shardId;
            this.request = request;
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            shardId = in.readInt();
            request = newRequestInstance();
            request.readFrom(in);
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeInt(shardId);
            request.writeTo(out);
        }
    }

    private class AsyncShardOperationAction {

        // 用于处理响应的ActionListener，它是一个回调接口，用于在操作完成时得到通知
        private final ActionListener<Response> listener;

        // 封装了请求信息的Request对象，它可能包含索引、查询或其他操作的详细信息
        private final Request request;

        // Nodes对象，包含有关Elasticsearch集群中节点的信息，如节点状态、属性等
        private Nodes nodes;

        // ShardsIterator对象，用于迭代请求所涉及的分片
        private ShardsIterator shards;

        // AtomicBoolean类型的变量，用于确保主分片上的操作只启动一次
        private final AtomicBoolean primaryOperationStarted = new AtomicBoolean();

        private AsyncShardOperationAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        public void start() {
            start(false);
        }

        /**
         * 检查是否开始在主分片上执行操作（或者已经完成）。
         * @param fromClusterEvent 是否由集群事件触发的操作
         * @return 如果操作已经开始或完成，则返回true。
         * @throws SearchException 搜索异常
         */
        public boolean start(final boolean fromClusterEvent) throws SearchException {
            // 获取当前集群状态
            ClusterState clusterState = clusterService.state();
            // 获取集群节点信息
            nodes = clusterState.nodes();
            try {
                // 获取涉及的分片迭代器
                shards = shards(request);
            } catch (Exception e) {
                // 如果获取分片失败，通过监听器回调失败
                listener.onFailure(new ShardOperationFailedException(shards.shardId(), e));
                return true;
            }

            boolean foundPrimary = false; // 标记是否找到主分片
            for (final ShardRouting shard : shards) { // 遍历分片
                if (shard.primary()) { // 如果是主分片
                    if (!shard.active()) { // 如果主分片未激活
                        retryPrimary(fromClusterEvent, shard); // 重试主分片操作
                        return false; // 操作尚未开始
                    }

                    // 确保主分片上的操作只启动一次
                    if (!primaryOperationStarted.compareAndSet(false, true)) {
                        return false; // 操作已经开始或完成
                    }

                    foundPrimary = true; // 标记找到主分片
                    if (shard.currentNodeId().equals(nodes.localNodeId())) { // 如果主分片在当前节点
                        // 根据是否需要线程化操作，执行主分片上的操作
                        if (request.operationThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    performOnPrimary(shard.id(), fromClusterEvent, true, shard);
                                }
                            });
                        } else {
                            performOnPrimary(shard.id(), fromClusterEvent, false, shard);
                        }
                    } else { // 如果主分片不在当前节点
                        // 向主分片所在节点发送请求
                        Node node = nodes.get(shard.currentNodeId());
                        transportService.sendRequest(node, transportAction(), request, new BaseTransportResponseHandler<Response>() {
                            // 实例化新响应对象
                            @Override public Response newInstance() {
                                return newResponseInstance();
                            }

                            // 处理响应
                            @Override public void handleResponse(Response response) {
                                listener.onResponse(response);
                            }

                            // 处理异常
                            @Override public void handleException(RemoteTransportException exp) {
                                listener.onFailure(exp);
                            }

                            // 是否需要在新线程中处理响应
                            @Override public boolean spawn() {
                                return request.listenerThreaded();
                            }
                        });
                    }
                    break; // 找到主分片后退出循环
                }
            }
            // 如果没有找到主分片，抛出异常
            if (!foundPrimary) {
                final PrimaryNotStartedActionException failure = new PrimaryNotStartedActionException(shards.shardId(), "Primary not found");
                // 根据是否需要线程化监听器，处理失败情况
                if (request.listenerThreaded()) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            listener.onFailure(failure);
                        }
                    });
                } else {
                    listener.onFailure(failure);
                }
            }
            return true; // 如果方法执行到这里，说明操作已经开始或完成
        }

        /**
         * 重试主分片上的操作。
         * @param fromClusterEvent 是否由集群事件触发的操作
         * @param shard 主分片的路由信息
         */
        private void retryPrimary(boolean fromClusterEvent, final ShardRouting shard) {
            // 如果不是由集群事件触发的操作
            if (!fromClusterEvent) {
                // 设置请求为线程化操作，以便我们可以在发现监听器线程上进行分叉
                request.operationThreaded(true);

                // 向集群服务添加一个超时监听器
                clusterService.add(request.timeout(), new TimeoutClusterStateListener() {
                    @Override
                    public void clusterChanged(ClusterChangedEvent event) {
                        // 如果start方法返回true，表示操作已经开始或完成
                        if (start(true)) {
                            // 如果我们成功启动并在主分片上执行了操作，我们可以移除这个监听器
                            clusterService.remove(this);
                        }
                    }

                    @Override
                    public void onTimeout(TimeValue timeValue) {
                        // 超时时创建一个新的PrimaryNotStartedActionException异常
                        final PrimaryNotStartedActionException failure = new PrimaryNotStartedActionException(shard.shardId(), "Timeout waiting for [" + timeValue + "]");
                        // 根据请求是否需要线程化监听器，处理失败情况
                        if (request.listenerThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    listener.onFailure(failure);
                                }
                            });
                        } else {
                            listener.onFailure(failure);
                        }
                    }
                });
            }
        }

        /**
         * 在主分片上执行操作。
         * @param primaryShardId 主分片的ID
         * @param fromDiscoveryListener 是否由发现监听器触发的操作
         * @param alreadyThreaded 是否已经是一个线程化操作
         * @param shard 主分片的路由信息
         */
        private void performOnPrimary(int primaryShardId, boolean fromDiscoveryListener, boolean alreadyThreaded, final ShardRouting shard) {
            try {
                // 创建一个新的ShardOperationRequest请求，并在主分片上执行操作
                Response response = shardOperationOnPrimary(new ShardOperationRequest(primaryShardId, request));
                // 如果主分片操作成功，继续执行备份分片上的操作
                performBackups(response, alreadyThreaded);
            } catch (IndexShardNotStartedException e) {
                // 如果索引分片尚未启动，仍在恢复中，则重试操作
                // 我们知道分片状态不是UNASSIGNED或INITIALIZING，因为在调用方法中已经检查过
                retryPrimary(fromDiscoveryListener, shard);
            } catch (Exception e) {
                // 如果捕获到其他异常，通过监听器回调失败
                listener.onFailure(new ShardOperationFailedException(shards.shardId(), e));
            }
        }

        /**
         * 在备份分片上执行操作。
         * @param response 主分片上操作的响应
         * @param alreadyThreaded 是否已经是一个线程化操作
         */
        private void performBackups(final Response response, boolean alreadyThreaded) {
            // 如果忽略备份分片或者没有备份分片（只有一个分片），则直接返回响应
            if (ignoreBackups() || shards.size() == 1 /* 没有备份 */) {
                // 根据是否需要线程化监听器，返回响应
                if (alreadyThreaded || !request.listenerThreaded()) {
                    listener.onResponse(response);
                } else {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            listener.onResponse(response);
                        }
                    });
                }
                return;
            }

            // 初始化计数器，用于跟踪备份分片操作的完成情况
            int backupCounter = 0;
            // 遍历分片，计算需要执行操作的备份分片数量
            for (final ShardRouting shard : shards.reset()) {
                if (shard.primary()) {
                    continue; // 跳过主分片
                }
                backupCounter++; // 非主分片计数
                // 如果备份分片正在迁移，也需要在其目标节点上执行操作
                if (shard.relocating()) {
                    backupCounter++;
                }
            }

            // 使用AtomicInteger以线程安全的方式递减计数器
            AtomicInteger counter = new AtomicInteger(backupCounter);
            // 再次遍历分片，执行备份分片上的操作
            for (final ShardRouting shard : shards.reset()) {
                if (shard.primary()) {
                    continue; // 跳过主分片
                }
                // 如果备份分片正在初始化，也执行操作
                if (shard.unassigned()) {
                    // 如果计数器减至0，说明所有备份分片操作已完成，返回响应
                    if (counter.decrementAndGet() == 0) {
                        if (alreadyThreaded || !request.listenerThreaded()) {
                            listener.onResponse(response);
                        } else {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    listener.onResponse(response);
                                }
                            });
                        }
                        break; // 跳出循环
                    }
                    continue; // 继续检查其他分片
                }
                // 对于非主分片，执行备份分片上的操作
                performOnBackup(response, counter, shard, shard.currentNodeId());
                // 如果分片正在迁移，也在目标节点上执行操作
                if (shard.relocating()) {
                    performOnBackup(response, counter, shard, shard.relocatingNodeId());
                }
            }
        }

        /**
         * 在备份分片上执行操作。
         * @param response 主分片上操作的响应
         * @param counter 用于跟踪所有备份分片操作完成情况的计数器
         * @param shard 当前分片的路由信息
         * @param nodeId 执行备份操作的目标节点ID
         */
        private void performOnBackup(final Response response, final AtomicInteger counter, final ShardRouting shard, String nodeId) {
            // 创建一个新的ShardOperationRequest请求
            final ShardOperationRequest shardRequest = new ShardOperationRequest(shards.shardId().id(), request);

            // 如果备份分片不在当前节点
            if (!nodeId.equals(nodes.localNodeId())) {
                Node node = nodes.get(nodeId); // 获取目标节点
                // 通过传输服务发送请求到备份节点
                transportService.sendRequest(node, transportBackupAction(), shardRequest, new VoidTransportResponseHandler() {
                    @Override
                    public void handleResponse(VoidStreamable vResponse) {
                        // 如果响应成功，调用finishIfPossible尝试完成操作
                        finishIfPossible();
                    }

                    @Override
                    public void handleException(RemoteTransportException exp) {
                        // 如果发生异常，首先检查是否忽略该异常
                        if (!ignoreBackupException(exp.unwrapCause())) {
                            logger.warn("Failed to perform " + transportAction() + " on backup " + shards.shardId(), exp);
                            shardStateAction.shardFailed(shard); // 标记分片失败
                        }
                        finishIfPossible(); // 调用finishIfPossible尝试完成操作
                    }

                    // 私有方法，用于在适当的时候完成操作
                    private void finishIfPossible() {
                        // 如果计数器减至0，说明所有备份分片操作已完成，返回响应
                        if (counter.decrementAndGet() == 0) {
                            if (request.listenerThreaded()) {
                                // 如果需要线程化监听器，则在线程池中执行
                                threadPool.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        listener.onResponse(response);
                                    }
                                });
                            } else {
                                listener.onResponse(response); // 直接返回响应
                            }
                        }
                    }

                    @Override
                    public boolean spawn() {
                        // 不需要在新线程中处理，如果需要线程化监听器，将在finishIfPossible中处理
                        return false;
                    }
                });
            } else {
                // 如果备份分片在当前节点，直接执行操作
                if (request.operationThreaded()) {
                    // 如果请求需要线程化操作，则在线程池中执行
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                shardOperationOnBackup(shardRequest); // 执行备份分片上的操作
                            } catch (Exception e) {
                                if (!ignoreBackupException(e)) {
                                    logger.warn("Failed to perform " + transportAction() + " on backup " + shards.shardId(), e);
                                    shardStateAction.shardFailed(shard); // 标记分片失败
                                }
                            }
                            // 如果计数器减至0，说明所有备份分片操作已完成，返回响应
                            if (counter.decrementAndGet() == 0) {
                                listener.onResponse(response);
                            }
                        }
                    });
                } else {
                    try {
                        shardOperationOnBackup(shardRequest); // 执行备份分片上的操作
                    } catch (Exception e) {
                        if (!ignoreBackupException(e)) {
                            logger.warn("Failed to perform " + transportAction() + " on backup " + shards.shardId(), e);
                            shardStateAction.shardFailed(shard); // 标记分片失败
                        }
                    }
                    // 如果计数器减至0，说明所有备份分片操作已完成，根据是否需要线程化监听器返回响应
                    if (counter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    listener.onResponse(response);
                                }
                            });
                        } else {
                            listener.onResponse(response);
                        }
                    }
                }
            }
        }

        /**
         * Should an exception be ignored when the operation is performed on the backup. The exception
         * is ignored if it is:
         *
         * <ul>
         * <li><tt>IllegalIndexShardStateException</tt>: The shard has not yet moved to started mode (it is still recovering).
         * <li><tt>IndexMissingException</tt>/<tt>IndexShardMissingException</tt>: The shard has not yet started to initialize on the target node.
         * </ul>
         */
        private boolean ignoreBackupException(Throwable e) {
            if (e instanceof IllegalIndexShardStateException) {
                return true;
            }
            if (e instanceof IndexMissingException) {
                return true;
            }
            if (e instanceof IndexShardMissingException) {
                return true;
            }
            return false;
        }
    }
}
