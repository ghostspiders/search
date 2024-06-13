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
import org.server.search.action.ActionListener;
import org.server.search.action.ActionResponse;
import org.server.search.action.Actions;
import org.server.search.action.support.BaseAction;
import org.server.search.cluster.ClusterService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

 
public abstract class TransportIndicesReplicationOperationAction<Request extends IndicesReplicationOperationRequest, Response extends ActionResponse, IndexRequest extends IndexReplicationOperationRequest, IndexResponse extends ActionResponse, ShardRequest extends ShardReplicationOperationRequest, ShardResponse extends ActionResponse>
        extends BaseAction<Request, Response> {

    protected final ThreadPool threadPool;

    protected final ClusterService clusterService;

    protected final TransportIndexReplicationOperationAction<IndexRequest, IndexResponse, ShardRequest, ShardResponse> indexAction;

    @Inject public TransportIndicesReplicationOperationAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                              TransportIndexReplicationOperationAction<IndexRequest, IndexResponse, ShardRequest, ShardResponse> indexAction) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indexAction = indexAction;

        transportService.registerHandler(transportAction(), new TransportHandler());
    }

    @Override
    protected void doExecute(final Request request, final ActionListener<Response> listener) {
        // 处理请求中指定的索引，获取有效的索引名称数组。
        String[] indices = Actions.processIndices(clusterService.state(), request.indices());
        // 创建一个原子整数计数器，用于跟踪每个索引请求的完成情况。
        final AtomicInteger indexCounter = new AtomicInteger();
        // 创建一个原子整数计数器，用于跟踪所有索引请求的完成情况。
        final AtomicInteger completionCounter = new AtomicInteger(indices.length);
        // 创建一个原子引用数组，用于存储每个索引的响应或异常。
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<Object>(indices.length);

        // 遍历所有索引。
        for (final String index : indices) {
            // 为每个索引创建一个新的索引请求实例。
            IndexRequest indexRequest = newIndexRequestInstance(request, index);
            // 设置索引请求不需要线程化的监听器。
            indexRequest.listenerThreaded(false);
            // 执行索引请求。
            indexAction.execute(indexRequest, new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse result) {
                    // 存储当前索引的响应结果。
                    indexResponses.set(indexCounter.getAndIncrement(), result);
                    // 如果所有索引请求都已完成，调用监听器的onResponse方法。
                    if (completionCounter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            // 如果请求需要线程化监听器，使用线程池执行。
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    listener.onResponse(newResponseInstance(request, indexResponses));
                                }
                            });
                        } else {
                            // 否则，直接调用onResponse方法。
                            listener.onResponse(newResponseInstance(request, indexResponses));
                        }
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    e.printStackTrace();
                    // 存储当前索引的异常。
                    int index = indexCounter.getAndIncrement();
                    if (accumulateExceptions()) {
                        indexResponses.set(index, e);
                    }
                    // 如果所有索引请求都已完成，调用监听器的onResponse方法。
                    if (completionCounter.decrementAndGet() == 0) {
                        if (request.listenerThreaded()) {
                            // 如果请求需要线程化监听器，使用线程池执行。
                            threadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    listener.onResponse(newResponseInstance(request, indexResponses));
                                }
                            });
                        } else {
                            // 否则，直接调用onResponse方法。
                            listener.onResponse(newResponseInstance(request, indexResponses));
                        }
                    }
                }
            });
        }
    }

    protected abstract Request newRequestInstance();

    protected abstract Response newResponseInstance(Request request, AtomicReferenceArray indexResponses);

    protected abstract String transportAction();

    protected abstract IndexRequest newIndexRequestInstance(Request request, String index);

    protected abstract boolean accumulateExceptions();

    private class TransportHandler extends BaseTransportRequestHandler<Request> {

        @Override public Request newInstance() {
            return newRequestInstance();
        }

        @Override public void messageReceived(final Request request, final TransportChannel channel) throws Exception {
            // no need for a threaded listener, since we just send a response
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
            // no need to spawn, since we always execute in the index one with threadedOperation set to true
            return false;
        }
    }
}