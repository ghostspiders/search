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
            // 计算本地操作的数量，并执行非本地操作
            int localOperations = 0;
            for (final ShardsIterator shardIt : shardsIts) {
                final ShardRouting shard = shardIt.next();
                // 检查分片是否处于活跃状态
                if (shard.active()) {
                    // 如果分片在当前节点上，则增加本地操作计数
                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                        localOperations++;
                    } else {
                        // 如果分片不在当前节点上，执行远程操作
                        // localAsync标志在此不相关
                        performFirstPhase(shardIt.reset());
                    }
                } else {
                    // 如果分片不活跃，就像我们遇到了一个问题
                    // 所以我们迭代到下一个分片并维护计数
                    onFirstPhaseResult(shard, shardIt, null);
                }
            }
            // 如果有本地操作，则现在执行它们
            if (localOperations > 0) {
                // 如果请求的操作线程模型是单线程，则在线程池中执行所有本地操作
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (final ShardsIterator shardIt : shardsIts) {
                                final ShardRouting shard = shardIt.reset().next();
                                if (shard.active()) {
                                    // 如果分片在当前节点上，则执行第一阶段
                                    if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                        performFirstPhase(shardIt.reset());
                                    }
                                }
                            }
                        }
                    });
                } else {
                    // 如果请求的操作线程模型是每个分片一个线程，则为每个本地分片创建一个新线程
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (final ShardsIterator shardIt : shardsIts) {
                        final ShardRouting shard = shardIt.reset().next();
                        if (shard.active()) {
                            // 如果分片在当前节点上
                            if (shard.currentNodeId().equals(nodes.localNodeId())) {
                                if (localAsync) {
                                    // 如果是每个分片一个线程的模型，则在线程池中执行
                                    threadPool.execute(new Runnable() {
                                        @Override public void run() {
                                            performFirstPhase(shardIt.reset());
                                        }
                                    });
                                } else {
                                    // 否则，直接执行第一阶段
                                    performFirstPhase(shardIt.reset());
                                }
                            }
                        }
                    }
                }
            }
        }

        /**
         * 执行搜索请求第一阶段的方法，针对每个分片执行。
         *
         * @param shardIt 分片路由迭代器，用于获取当前分片的路由信息。
         */
        private void performFirstPhase(final Iterator<ShardRouting> shardIt) {
            // 检查迭代器是否还有更多的分片路由对象。
            if (!shardIt.hasNext()) {
                // 如果没有更多分片，结束方法。
                return;
            }

            // 获取迭代器中的下一个分片路由。
            final ShardRouting shard = shardIt.next();

            // 检查分片是否处于活跃状态。
            if (!shard.active()) {
                // 如果分片不活跃，调用onFirstPhaseResult方法处理不活跃的分片情况。
                // 这可能表示分片当前不可用或正在恢复。
                onFirstPhaseResult(shard, shardIt, null);
            } else {
                // 如果分片是活跃的，获取与分片关联的节点。
                Node node = nodes.get(shard.currentNodeId());

                // 发送执行第一阶段的请求到节点。
                // 这里使用SearchServiceListener来处理执行结果或失败。
                sendExecuteFirstPhase(node, internalSearchRequest(shard, request), new SearchServiceListener<FirstResult>() {
                    @Override
                    public void onResult(FirstResult result) {
                        // 如果第一阶段成功执行，调用onFirstPhaseResult方法，并传递结果。
                        onFirstPhaseResult(shard, result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // 如果执行失败，调用onFirstPhaseResult方法，并传递异常信息。
                        onFirstPhaseResult(shard, shardIt, t);
                    }
                });
            }
        }

        /**
         * 处理搜索请求第一阶段的结果。
         *
         * @param shard 分片路由，包含分片的相关信息。
         * @param result 第一阶段的结果，可能包含查询结果或状态信息。
         */
        private void onFirstPhaseResult(ShardRouting shard, FirstResult result) {
            // 处理第一阶段的结果。
            // 这个方法可能包括合并结果、更新状态或其他必要的操作。
            processFirstPhaseResult(shard, result);

            // 当当前成功操作数增加后等于预期的成功操作数，
            // 或者总操作数增加后等于预期的总操作数时，
            // 触发第二阶段的开始。
            // successulOps 是成功操作的原子计数器，expectedSuccessfulOps 是预期的成功操作数。
            // totalOps 是总操作的原子计数器，expectedTotalOps 是预期的总操作数。
            if (successulOps.incrementAndGet() == expectedSuccessfulOps ||
                    totalOps.incrementAndGet() == expectedTotalOps) {
                // 调用 moveToSecondPhase 方法，准备进入搜索请求的第二阶段。
                moveToSecondPhase();
            }
        }

        /**
         * 处理搜索请求第一阶段的结果，无论是成功还是失败。
         *
         * @param shard 当前处理的分片路由。
         * @param shardIt 分片路由的迭代器，用于在需要时继续执行其他分片。
         * @param t 可能发生的异常，如果第一阶段失败则不为null。
         */
        private void onFirstPhaseResult(ShardRouting shard, final Iterator<ShardRouting> shardIt, Throwable t) {
            // 如果日志记录器处于调试模式，并且有异常发生，则记录异常信息。
            if (logger.isDebugEnabled()) {
                if (t != null) {
                    // 记录分片的简短摘要、搜索请求的失败信息和异常堆栈跟踪。
                    logger.debug(shard.shortSummary() + ": Failed to search [" + request + "]", t);
                }
            }

            // 如果总操作数增加后等于预期的总操作数，则触发第二阶段的开始。
            if (totalOps.incrementAndGet() == expectedTotalOps) {
                moveToSecondPhase();
            } else {
                // 如果还有更多的分片需要处理，则继续执行第一阶段。
                performFirstPhase(shardIt);
            }
        }

        /**
         * 发送执行搜索请求第一阶段的请求到指定节点。
         * 这是一个抽象方法，需要在子类中具体实现。
         *
         * @param node 要发送请求的目标节点。
         * @param request 内部搜索请求对象。
         * @param listener 搜索服务监听器，用于接收结果或异常。
         */
        protected abstract void sendExecuteFirstPhase(Node node, InternalSearchRequest request, SearchServiceListener<FirstResult> listener);

        /**
         * 处理搜索请求第一阶段的结果。
         * 这是一个抽象方法，需要在子类中具体实现。
         *
         * @param shard 分片路由，包含分片的相关信息。
         * @param result 第一阶段的结果。
         */
        protected abstract void processFirstPhaseResult(ShardRouting shard, FirstResult result);

        /**
         * 触发搜索请求的第二阶段。
         * 这是一个抽象方法，需要在子类中具体实现。
         */
        protected abstract void moveToSecondPhase();
    }
}
