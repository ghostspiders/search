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

package org.server.search.action.support.single;

import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.action.ActionResponse;
import org.server.search.action.NoShardAvailableActionException;
import org.server.search.action.support.BaseAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.*;
import org.server.search.util.io.Streamable;
import org.server.search.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

 
public abstract class TransportSingleOperationAction<Request extends SingleOperationRequest, Response extends ActionResponse> extends BaseAction<Request, Response> {

    /**
     * 集群服务（ClusterService），用于管理Elasticsearch集群的状态。
     * 它允许节点了解集群的当前状态，包括哪些节点是主节点，以及集群的元数据和设置。
     */
    protected final ClusterService clusterService;

    /**
     * 传输服务（TransportService），负责节点间的通信。
     * 它处理节点之间的请求和响应，是Elasticsearch分布式操作的基石。
     */
    protected final TransportService transportService;

    /**
     * 索引服务（IndicesService），管理所有索引的操作。
     * 它负责创建和删除索引，以及管理与索引相关的各种操作，如映射、设置等。
     */
    protected final IndicesService indicesService;

    /**
     * 线程池（ThreadPool），用于执行Elasticsearch中的并发任务。
     * 它允许Elasticsearch以异步和多线程的方式处理各种操作，提高性能和响应能力。
     */
    protected final ThreadPool threadPool;

    protected TransportSingleOperationAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.indicesService = indicesService;

        transportService.registerHandler(transportAction(), new TransportHandler());
        transportService.registerHandler(transportShardAction(), new ShardTransportHandler());
    }

    @Override protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSingleAction(request, listener).start();
    }

    /**
     * 返回传输层操作的名称。
     * 这个名称用于在节点间传输时标识特定的操作行为。
     * @return 传输层操作的名称
     */
    protected abstract String transportAction();

    /**
     * 返回针对分片的操作的传输层名称。
     * 这通常用于在集群中的分片上执行操作时的通信。
     * @return 分片操作的传输层名称
     */
    protected abstract String transportShardAction();

    /**
     * 在指定的分片上执行操作，并返回响应。
     * 这个方法需要子类具体实现，它定义了如何在一个分片上执行操作。
     * @param request 请求对象，包含执行操作所需的信息
     * @param shardId 分片的ID
     * @return 执行操作后的响应对象
     * @throws SearchException 如果在执行操作时发生搜索相关的异常
     */
    protected abstract Response shardOperation(Request request, int shardId) throws SearchException;

    /**
     * 创建并返回一个新的请求对象。
     * 子类需要实现这个方法，以便为特定的操作创建合适的请求对象。
     * @return 新的请求对象
     */
    protected abstract Request newRequest();

    /**
     * 创建并返回一个新的响应对象。
     * 子类需要实现这个方法，以便为特定的操作创建合适的响应对象。
     * @return 新的响应对象
     */
    protected abstract Response newResponse();

    private class AsyncSingleAction {

        /**
         * 用于处理响应的ActionListener，它是一个回调接口，用于在操作完成时得到通知。
         * 这个成员变量被声明为final，表示一旦在构造函数中初始化后，就不能被再次赋值。
         */
        private final ActionListener<Response> listener;

        /**
         * 包含分片迭代器的ShardsIterator对象，用于访问和迭代相关的分片。
         * 这个对象提供了对分片信息的访问，并允许按需获取分片的状态和节点信息。
         */
        private final ShardsIterator shards;

        /**
         * 分片路由的迭代器，用于遍历包含在ShardsIterator中的ShardRouting对象。
         * 这个迭代器允许逐个处理每个分片路由，以执行特定的操作。
         */
        private Iterator<ShardRouting> shardsIt;

        /**
         * 封装了请求信息的Request对象，它可能包含索引、查询或其他操作的详细信息。
         * 这个成员变量被声明为final，表示它在初始化之后不能被再次赋值。
         */
        private final Request request;

        /**
         * Nodes对象，包含有关Elasticsearch集群中节点的信息，如节点状态、属性等。
         * 这个对象使得可以在操作中访问和使用集群的节点信息。
         */
        private final Nodes nodes;
        private AsyncSingleAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();

            nodes = clusterState.nodes();

            this.shards = indicesService.indexServiceSafe(request.index).operationRouting()
                    .getShards(clusterState, request.type(), request.id());
            this.shardsIt = shards.iterator();
        }

        public void start() {
            performFirst();
        }

        public void onFailure(ShardRouting shardRouting, Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(shardRouting.shortSummary() + ": Failed to get [" + request.type() + "#" + request.id() + "]", e);
            }
            perform(e);
        }
        /**
         * 执行首次获取操作，首选尝试使用本地节点的分片以提高性能。
         */
        private void performFirst() {
            // 循环遍历分片迭代器
            while (shardsIt.hasNext()) {
                final ShardRouting shard = shardsIt.next(); // 获取下一个分片路由
                // 如果分片未激活，跳过
                if (!shard.active()) {
                    continue;
                }
                // 如果分片当前所在的节点是本地节点
                if (shard.currentNodeId().equals(nodes.localNodeId())) {
                    // 如果请求是线程操作
                    if (request.threadedOperation()) {
                        // 在线程池中执行请求操作
                        threadPool.execute(new Runnable() {
                            @Override public void run() {
                                try {
                                    // 执行分片操作并获取响应
                                    Response response = shardOperation(request, shard.id());
                                    // 回调监听器的onResponse方法
                                    listener.onResponse(response);
                                } catch (Exception e) {
                                    // 如果执行过程中出现异常，调用onFailure方法
                                    onFailure(shard, e);
                                }
                            }
                        });
                        // 执行完毕后返回
                        return;
                    } else {
                        // 如果请求不是线程操作
                        try {
                            // 执行分片操作并获取响应
                            final Response response = shardOperation(request, shard.id());
                            // 如果请求需要线程化监听器回调
                            if (request.listenerThreaded()) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        // 在线程池中回调监听器的onResponse方法
                                        listener.onResponse(response);
                                    }
                                });
                            } else {
                                // 直接回调监听器的onResponse方法
                                listener.onResponse(response);
                            }
                            // 执行完毕后返回
                            return;
                        } catch (Exception e) {
                            // 如果执行过程中出现异常，调用onFailure方法
                            onFailure(shard, e);
                        }
                    }
                }
            }
            // 如果迭代器中没有更多的本地分片，即所有本地分片都已遍历完毕
            if (!shardsIt.hasNext()) {
                // 重置分片迭代器，准备进行远程获取操作
                shardsIt = shards.reset().iterator();
                // 执行远程获取操作
                perform(null);
            }
        }

        /**
         * 执行分片操作，尝试在非本地节点上执行请求，如果所有尝试都失败，则报告异常。
         * @param lastException 最后一次捕获的异常，用于在所有分片尝试失败时报告
         */
        private void perform(final Exception lastException) {
            // 循环遍历分片迭代器中的分片路由
            while (shardsIt.hasNext()) {
                final ShardRouting shard = shardsIt.next();
                // 如果分片未激活，则跳过
                if (!shard.active()) {
                    continue;
                }
                // 由于已经在performFirst中尝试过本地节点，这里不需要再次检查
                if (!shard.currentNodeId().equals(nodes.localNodeId())) {
                    // 获取远程节点信息
                    Node node = nodes.get(shard.currentNodeId());
                    // 通过传输服务发送请求到远程节点
                    transportService.sendRequest(node, transportShardAction(),
                            new ShardSingleOperationRequest(request, shard.id()),
                            new BaseTransportResponseHandler<Response>() {
                                // 实例化一个新的响应对象
                                @Override public Response newInstance() {
                                    return newResponse();
                                }

                                // 处理来自远程节点的响应
                                @Override public void handleResponse(final Response response) {
                                    // 根据请求是否需要线程化监听器回调，执行相应的操作
                                    if (request.listenerThreaded()) {
                                        threadPool.execute(new Runnable() {
                                            @Override public void run() {
                                                listener.onResponse(response);
                                            }
                                        });
                                    } else {
                                        listener.onResponse(response);
                                    }
                                }

                                // 处理来自远程节点的异常
                                @Override public void handleException(RemoteTransportException exp) {
                                    // 调用onFailure方法，并传递分片路由和异常信息
                                    onFailure(shard, exp);
                                }

                                // 是否需要在新线程中处理请求
                                @Override public boolean spawn() {
                                    // 不需要在新线程中处理，响应时会根据需要在不同线程执行监听器
                                    return false;
                                }
                            });
                    // 发送请求后返回，等待响应或异常处理
                    return;
                }
            }
            // 如果分片迭代器中没有更多分片，即所有分片尝试失败
            if (!shardsIt.hasNext()) {
                // 创建一个NoShardAvailableActionException异常，表示没有可用的分片
                final NoShardAvailableActionException failure = new NoShardAvailableActionException(
                        shards.shardId(),
                        "No shard available for [" + request.type() + "#" + request.id() + "]",
                        lastException
                );
                // 根据请求是否需要线程化监听器回调，执行失败回调
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
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequest();
        }

        @Override public void messageReceived(Request request, final TransportChannel channel) throws Exception {
            // no need to have a threaded listener since we just send back a response
            request.listenerThreaded(false);
            // if we have a local operation, execute it on a thread since we don't spawn
            request.threadedOperation(true);
            execute(request, new ActionListener<Response>() {
                @Override public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send response for get", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            return false;
        }
    }

    private class ShardTransportHandler extends BaseTransportRequestHandler<ShardSingleOperationRequest> {

        @Override public ShardSingleOperationRequest newInstance() {
            return new ShardSingleOperationRequest();
        }

        @Override public void messageReceived(ShardSingleOperationRequest request, TransportChannel channel) throws Exception {
            Response response = shardOperation(request.request(), request.shardId());
            channel.sendResponse(response);
        }
    }

    protected class ShardSingleOperationRequest implements Streamable {

        private Request request;

        private int shardId;

        ShardSingleOperationRequest() {
        }

        public ShardSingleOperationRequest(Request request, int shardId) {
            this.request = request;
            this.shardId = shardId;
        }

        public Request request() {
            return request;
        }

        public int shardId() {
            return shardId;
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            request = newRequest();
            request.readFrom(in);
            shardId = in.readInt();
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            request.writeTo(out);
            out.writeInt(shardId);
        }
    }
}
