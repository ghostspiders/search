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

package org.server.search.action.search.type;

import org.server.search.action.ActionListener;
import org.server.search.action.search.SearchOperationThreading;
import org.server.search.action.search.SearchRequest;
import org.server.search.action.search.SearchResponse;
import org.server.search.action.support.BaseAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.GroupShardsIterator;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.indices.IndicesService;
import org.server.search.search.action.SearchServiceListener;
import org.server.search.search.action.SearchServiceTransportAction;
import org.server.search.search.controller.SearchPhaseController;
import org.server.search.search.controller.ShardDoc;
import org.server.search.search.internal.InternalSearchRequest;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.settings.Settings;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.server.search.action.Actions.*;
import static org.server.search.action.search.type.TransportSearchHelper.*;


public abstract class TransportSearchTypeAction extends BaseAction<SearchRequest, SearchResponse> {

    /**
     * 线程池服务，用于管理Elasticsearch节点中的任务执行。
     * 线程池允许定制不同任务类型的线程和队列配置。
     */
    protected final ThreadPool threadPool;

    /**
     * 集群服务，提供集群级别的操作和管理。
     * 负责维护集群状态和处理集群元数据变更。
     */
    protected final ClusterService clusterService;

    /**
     * 索引服务，管理Elasticsearch中的索引生命周期。
     * 负责索引的创建、删除以及分片分配等操作。
     */
    protected final IndicesService indicesService;

    /**
     * 搜索服务传输动作，处理搜索请求的接收和转发。
     * 它是Elasticsearch搜索功能的核心组件之一。
     */
    protected final SearchServiceTransportAction searchService;

    /**
     * 搜索阶段控制器，管理搜索请求的执行流程。
     * 控制包括查询、排序等在内的不同搜索阶段。
     */
    protected final SearchPhaseController searchPhaseController;

    /**
     * 传输搜索缓存，可能用于缓存搜索结果以提高性能。
     * 通过缓存可以减少重复搜索的计算量，加快响应速度。
     */
    protected final TransportSearchCache transportSearchCache;

    public TransportSearchTypeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                     TransportSearchCache transportSearchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportSearchCache = transportSearchCache;
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.searchPhaseController = searchPhaseController;
    }

    protected abstract class BaseAsyncAction<FirstResult> {

        /**
         * 用于接收搜索操作结果的监听器。
         * 当搜索请求完成时，该监听器会被触发。
         */
        protected final ActionListener<SearchResponse> listener;

        /**
         * 一个迭代器，用于遍历涉及的分片组。
         * 它允许按需访问分片信息，用于搜索操作。
         */
        protected final GroupShardsIterator shardsIts;

        /**
         * 封装了搜索请求的详细信息。
         * 包括查询条件、搜索类型等。
         */
        protected final SearchRequest request;

        /**
         * 节点服务，提供有关Elasticsearch集群节点的信息和管理。
         * 可用于与集群中的节点进行通信。
         */
        protected final Nodes nodes;

        /**
         * 预期成功的操作数量。
         * 用于跟踪搜索请求中预期成功的操作总数。
         */
        protected final int expectedSuccessfulOps;

        /**
         * 预期的总操作数量。
         * 包括预期成功和可能失败的操作。
         */
        protected final int expectedTotalOps;

        /**
         * 成功操作的原子计数器。
         * 用于线程安全地跟踪成功的搜索操作数量。
         */
        protected final AtomicInteger successulOps = new AtomicInteger();

        /**
         * 总操作的原子计数器。
         * 用于线程安全地跟踪搜索请求的总操作数量。
         */
        protected final AtomicInteger totalOps = new AtomicInteger();

        /**
         * 已排序的分片列表。
         * 这个数组可能保存了排序后的分片信息，用于搜索过程中的负载均衡或优化。
         */
        protected volatile ShardDoc[] sortedShardList;

        protected BaseAsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();

            nodes = clusterState.nodes();

            shardsIts = indicesService.searchShards(clusterState, processIndices(clusterState, request.indices()), request.queryHint());
            expectedSuccessfulOps = shardsIts.size();
            expectedTotalOps = shardsIts.totalSize();
        }

        public void start() {
            // count the local operations, and perform the non local ones
            int localOperations = 0;
            for (final ShardsIterator shardIt : shardsIts) {
                final ShardRouting shard = shardIt.next();
                if (shard.active()) {
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // do the remote operation here, the localAsync flag is not relevant
                        performFirstPhase(shardIt.reset());
                    }
                } else {
                    // as if we have a "problem", so we iterate to the next one and maintain counts
                    onFirstPhaseResult(shard, shardIt, null);
                }
            }
            // we have local operations, perform them now
            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (final ShardsIterator shardIt : shardsIts) {
                                final ShardRouting shard = shardIt.reset().next();
                                if (shard.active()) {
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performFirstPhase(shardIt.reset());
                                    }
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (final ShardsIterator shardIt : shardsIts) {
                        final ShardRouting shard = shardIt.reset().next();
                        if (shard.active()) {
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                if (localAsync) {
                                    threadPool.execute(new Runnable() {
                                        @Override public void run() {
                                            performFirstPhase(shardIt.reset());
                                        }
                                    });
                                } else {
                                    performFirstPhase(shardIt.reset());
                                }
                            }
                        }
                    }
                }
            }
        }

        private void performFirstPhase(final Iterator<ShardRouting> shardIt) {
            if (!shardIt.hasNext()) {
                return;
            }
            final ShardRouting shard = shardIt.next();
            if (!shard.active()) {
                // as if we have a "problem", so we iterate to the next one and maintain counts
                onFirstPhaseResult(shard, shardIt, null);
            } else {
                Node node = nodes.get(shard.currentNodeId());
                sendExecuteFirstPhase(node, internalSearchRequest(shard, request), new SearchServiceListener<FirstResult>() {
                    @Override public void onResult(FirstResult result) {
                        onFirstPhaseResult(shard, result);
                    }

                    @Override public void onFailure(Throwable t) {
                        onFirstPhaseResult(shard, shardIt, t);
                    }
                });
            }
        }

        private void onFirstPhaseResult(ShardRouting shard, FirstResult result) {
            processFirstPhaseResult(shard, result);
            if (successulOps.incrementAndGet() == expectedSuccessfulOps ||
                    totalOps.incrementAndGet() == expectedTotalOps) {
                moveToSecondPhase();
            }
        }

        private void onFirstPhaseResult(ShardRouting shard, final Iterator<ShardRouting> shardIt, Throwable t) {
            if (logger.isDebugEnabled()) {
                if (t != null) {
                    logger.debug(shard.shortSummary() + ": Failed to search [" + request + "]", t);
                }
            }
            if (totalOps.incrementAndGet() == expectedTotalOps) {
                moveToSecondPhase();
            } else {
                performFirstPhase(shardIt);
            }
        }

        protected abstract void sendExecuteFirstPhase(Node node, InternalSearchRequest request, SearchServiceListener<FirstResult> listener);

        protected abstract void processFirstPhaseResult(ShardRouting shard, FirstResult result);

        protected abstract void moveToSecondPhase();
    }
}
