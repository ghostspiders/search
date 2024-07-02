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

package org.server.search.action.support.broadcast;

import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.action.ShardOperationFailedException;
import org.server.search.action.support.BaseAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.GroupShardsIterator;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.*;
import org.server.search.util.settings.Settings;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.server.search.action.Actions.*;

 
public abstract class TransportBroadcastOperationAction<Request extends BroadcastOperationRequest, Response extends BroadcastOperationResponse, ShardRequest extends BroadcastShardOperationRequest, ShardResponse extends BroadcastShardOperationResponse>
        extends BaseAction<Request, Response> {

    protected final ClusterService clusterService;

    protected final TransportService transportService;

    protected final IndicesService indicesService;

    protected final ThreadPool threadPool;

    protected TransportBroadcastOperationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;

        transportService.registerHandler(transportAction(), new TransportHandler());
        transportService.registerHandler(transportShardAction(), new ShardTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncBroadcastAction(request, listener).start();
    }

    protected abstract String transportAction();

    protected abstract String transportShardAction();

    protected abstract Request newRequest();

    protected abstract Response newResponse(Request request, AtomicReferenceArray shardsResponses);

    protected abstract ShardRequest newShardRequest();

    protected abstract ShardRequest newShardRequest(ShardRouting shard, Request request);

    protected abstract ShardResponse newShardResponse();

    protected abstract ShardResponse shardOperation(ShardRequest request) throws SearchException;

    protected abstract boolean accumulateExceptions();

    private class AsyncBroadcastAction {

        // 声明一个私有且不可变的 Request 对象，存储请求的相关信息。
        private final Request request;

        // 声明一个私有且不可变的 ActionListener 对象，用于异步处理响应或异常。
        private final ActionListener<Response> listener;

        // 声明一个私有且不可变的 Nodes 对象，可能包含集群节点的相关信息。
        private final Nodes nodes;

        // 声明一个私有且不可变的 GroupShardsIterator 对象，用于迭代索引的分片。
        private final GroupShardsIterator shardsIts;

        // 声明一个整型变量，表示预期的操作数量。
        private final int expectedOps;

        // 声明一个原子整型变量，用于以线程安全的方式计数完成的操作数量。
        private final AtomicInteger counterOps = new AtomicInteger();

        // 声明另一个原子整型变量，可能用于跟踪索引过程中的特定计数。
        private final AtomicInteger indexCounter = new AtomicInteger();

        // 声明一个原子引用数组，用于以线程安全的方式存储每个分片的响应。
        private final AtomicReferenceArray shardsResponses;

        private AsyncBroadcastAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();
            nodes = clusterState.nodes();
            shardsIts = indicesService.searchShards(clusterState, processIndices(clusterState, request.indices()), request.queryHint());
            expectedOps = shardsIts.size();


            shardsResponses = new AtomicReferenceArray<Object>(expectedOps);
        }

        public void start() {
            // 计数本地操作的数量
            int localOperations = 0;
            // 遍历所有的分片迭代器
            for (final ShardsIterator shardIt : shardsIts) {
                // 获取下一个分片路由
                final ShardRouting shard = shardIt.next();
                // 检查分片是否处于活跃状态
                if (shard.active()) {
                    // 如果分片在当前节点上，则增加本地操作计数
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // 如果分片不在当前节点上，执行远程操作
                        // localAsync标志在此不相关
                        performOperation(shardIt.reset(), true);
                    }
                } else {
                    // 如果分片不活跃，就像遇到了问题一样，继续迭代到下一个分片并维护计数
                    onOperation(shard, shardIt, null, false);
                }
            }
            // 如果有本地操作，现在执行它们
            if (localOperations > 0) {
                // 根据请求的操作线程模式决定如何执行
                if (request.operationThreading() == BroadcastOperationThreading.SINGLE_THREAD) {
                    // 如果是单线程模式，在线程池中执行
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            // 再次遍历分片迭代器
                            for (final ShardsIterator shardIt : shardsIts) {
                                final ShardRouting shard = shardIt.reset().next();
                                if (shard.active()) {
                                    // 如果分片在当前节点上，执行本地操作
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performOperation(shardIt.reset(), false);
                                    }
                                }
                            }
                        }
                    });
                } else {
                    // 如果是每个分片一个线程或不使用线程的模式
                    boolean localAsync = request.operationThreading() == BroadcastOperationThreading.THREAD_PER_SHARD;
                    // 遍历分片迭代器，执行本地异步操作
                    for (final ShardsIterator shardIt : shardsIts) {
                        final ShardRouting shard = shardIt.reset().next();
                        if (shard.active()) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                performOperation(shardIt.reset(), localAsync);
                            }
                        }
                    }
                }
            }
        }

        // 执行分片操作的方法
        private void performOperation(final Iterator<ShardRouting> shardIt, boolean localAsync) {
            // 获取迭代器中的下一个分片路由对象
            final ShardRouting shard = shardIt.next();
            // 如果分片不处于激活状态，就当作有问题，迭代到下一个分片并维护计数
            if (!shard.active()) {
                onOperation(shard, shardIt, null, false);
            } else {
                // 为当前分片创建一个分片请求
                final ShardRequest shardRequest = newShardRequest(shard, request);
                // 如果分片的当前节点ID与本地节点ID相同
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    // 如果是本地异步执行
                    if (localAsync) {
                        threadPool.execute(new Runnable() {
                            @Override public void run() {
                                try {
                                    // 执行分片操作并处理结果
                                    onOperation(shard, shardOperation(shardRequest), true);
                                } catch (Exception e) {
                                    // 如果执行过程中出现异常，处理异常
                                    onOperation(shard, shardIt, e, true);
                                }
                            }
                        });
                    } else {
                        try {
                            // 同步执行分片操作并处理结果
                            onOperation(shard, shardOperation(shardRequest), false);
                        } catch (Exception e) {
                            // 如果执行过程中出现异常，处理异常
                            onOperation(shard, shardIt, e, false);
                        }
                    }
                } else {
                    // 获取分片当前节点对象
                    Node node = nodes.get(shard.currentNodeId());
                    // 向分片所在节点发送请求
                    transportService.sendRequest(node, transportShardAction(), shardRequest, new BaseTransportResponseHandler<ShardResponse>() {
                        @Override public ShardResponse newInstance() {
                            // 创建一个新的分片响应实例
                            return newShardResponse();
                        }

                        @Override public void handleResponse(ShardResponse response) {
                            // 处理来自远程节点的响应
                            onOperation(shard, response, false);
                        }

                        @Override public void handleException(RemoteTransportException exp) {
                            // 处理远程传输过程中的异常
                            onOperation(shard, shardIt, exp, false);
                        }

                        @Override public boolean spawn() {
                            // 这里我们不进行额外的线程分叉，如果需要在onOperation中处理
                            return false;
                        }
                    });
                }
            }
        }

        private void onOperation(ShardRouting shard, ShardResponse response, boolean alreadyThreaded) {
            shardsResponses.set(indexCounter.getAndIncrement(), response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(alreadyThreaded);
            }
        }

        private void onOperation(ShardRouting shard, final Iterator<ShardRouting> shardIt, Exception e, boolean alreadyThreaded) {
            if (logger.isDebugEnabled()) {
                if (e != null) {
                    logger.debug(shard.shortSummary() + ": Failed to execute [" + request + "]", e);
                }
            }
            if (!shardIt.hasNext()) {
                // no more shards in this partition
                int index = indexCounter.getAndIncrement();
                if (accumulateExceptions()) {
                    shardsResponses.set(index, new ShardOperationFailedException(shard.shardId(), e));
                }
                if (expectedOps == counterOps.incrementAndGet()) {
                    finishHim(alreadyThreaded);
                }
                return;
            }
            // we are not threaded here if we got here from the transport
            // or we possibly threaded if we got from a local threaded one,
            // in which case, the next shard in the partition will not be local one
            // so there is no meaning to this flag
            performOperation(shardIt, true);
        }

        private void finishHim(boolean alreadyThreaded) {
            // if we need to execute the listener on a thread, and we are not threaded already
            // then do it
            if (request.listenerThreaded() && !alreadyThreaded) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        listener.onResponse(newResponse(request, shardsResponses));
                    }
                });
            } else {
                listener.onResponse(newResponse(request, shardsResponses));
            }
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequest();
        }

        @Override public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // we just send back a response, no need to fork a listener
            request.listenerThreaded(false);
            // we don't spawn, so if we get a request with no threading, change it to single threaded
            if (request.operationThreading() == BroadcastOperationThreading.NO_THREADS) {
                request.operationThreading(BroadcastOperationThreading.SINGLE_THREAD);
            }
            execute(request, new ActionListener<Response>() {
                @Override public void onResponse(Response response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class ShardTransportHandler extends BaseTransportRequestHandler<ShardRequest> {

        @Override public ShardRequest newInstance() {
            return newShardRequest();
        }

        @Override public void messageReceived(ShardRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(shardOperation(request));
        }
    }
}
