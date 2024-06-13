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

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.action.ActionResponse;
import org.server.search.action.support.BaseAction;
import org.server.search.cluster.routing.GroupShardsIterator;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

 
public abstract class TransportIndexReplicationOperationAction<Request extends IndexReplicationOperationRequest, Response extends ActionResponse, ShardRequest extends ShardReplicationOperationRequest, ShardResponse extends ActionResponse>
        extends BaseAction<Request, Response> {

    protected final ThreadPool threadPool;

    protected final TransportShardReplicationOperationAction<ShardRequest, ShardResponse> shardAction;

    @Inject public TransportIndexReplicationOperationAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                                            TransportShardReplicationOperationAction<ShardRequest, ShardResponse> shardAction) {
        super(settings);
        this.threadPool = threadPool;
        this.shardAction = shardAction;

        transportService.registerHandler(transportAction(), new TransportHandler());
    }

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        GroupShardsIterator groups;
        try {
            // 尝试获取请求涉及的分片迭代器。
            groups = shards(request);
        } catch (Exception e) {
            // 如果获取分片迭代器时出现异常，使用监听器的onFailure方法通知失败。
            listener.onFailure(e);
            return;
        }
        // 创建原子整数计数器，用于跟踪每个分片请求的完成情况。
        final AtomicInteger indexCounter = new AtomicInteger();
        // 创建原子整数计数器，用于跟踪所有分片请求的完成情况。
        final AtomicInteger completionCounter = new AtomicInteger(groups.size());
        // 创建原子引用数组，用于存储每个分片的响应或异常。
        final AtomicReferenceArray<Object> shardsResponses = new AtomicReferenceArray<Object>(groups.size());

        // 遍历所有分片迭代器。
        for (final ShardsIterator shards : groups) {
            // 为每个分片创建一个新的分片请求实例。
            ShardRequest shardRequest = newShardRequestInstance(request, shards.shardId().id());
            // 目前，我们对每个索引的分片操作进行分叉处理。
            shardRequest.operationThreaded(true);
            // 不需要线程化的监听器，我们将在完成后基于索引请求进行分叉。
            shardRequest.listenerThreaded(false);
            // 执行分片请求。
            shardAction.execute(shardRequest, new ActionListener<ShardResponse>() {
                @Override
                public void onResponse(ShardResponse result) {
                    // 存储当前分片的响应结果。
                    shardsResponses.set(indexCounter.getAndIncrement(), result);
                    // 如果所有分片请求都已完成，调用监听器的onResponse方法。
                    if (completionCounter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            // 如果请求需要线程化监听器，使用线程池执行。
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    listener.onResponse(newResponseInstance(request, shardsResponses));
                                }
                            });
                        } else {
                            // 否则，直接调用onResponse方法。
                            listener.onResponse(newResponseInstance(request, shardsResponses));
                        }
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    // 存储当前分片的异常。
                    int index = indexCounter.getAndIncrement();
                    if (accumulateExceptions()) {
                        shardsResponses.set(index, e);
                    }
                    // 如果所有分片请求都已完成，调用监听器的onResponse方法。
                    if (completionCounter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            // 如果请求需要线程化监听器，使用线程池执行。
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    listener.onResponse(newResponseInstance(request, shardsResponses));
                                }
                            });
                        } else {
                            // 否则，直接调用onResponse方法。
                            listener.onResponse(newResponseInstance(request, shardsResponses));
                        }
                    }
                }
            });
        }
    }

    protected abstract Request newRequestInstance();

    protected abstract Response newResponseInstance(Request request, AtomicReferenceArray shardsResponses);

    protected abstract String transportAction();

    protected abstract GroupShardsIterator shards(Request request) throws SearchException;

    protected abstract ShardRequest newShardRequestInstance(Request request, int shardId);

    protected abstract boolean accumulateExceptions();

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequestInstance();
        }

        @Override public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // no need to use threaded listener, since we just send a response
            request.listenerThreaded(false);
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
                        logger.warn("Failed to send error response for action [" + transportAction() + "] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override public boolean spawn() {
            // no need to spawn, since in the doExecute we always execute with threaded operation set to true
            return false;
        }
    }
}