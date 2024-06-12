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

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.search.SearchOperationThreading;
import org.server.search.action.search.SearchRequest;
import org.server.search.action.search.SearchResponse;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.indices.IndicesService;
import org.server.search.search.SearchShardTarget;
import org.server.search.search.action.SearchServiceListener;
import org.server.search.search.action.SearchServiceTransportAction;
import org.server.search.search.controller.SearchPhaseController;
import org.server.search.search.dfs.AggregatedDfs;
import org.server.search.search.dfs.DfsSearchResult;
import org.server.search.search.fetch.FetchSearchRequest;
import org.server.search.search.fetch.FetchSearchResult;
import org.server.search.search.internal.InternalSearchRequest;
import org.server.search.search.internal.InternalSearchResponse;
import org.server.search.search.query.QuerySearchRequest;
import org.server.search.search.query.QuerySearchResult;
import org.server.search.search.query.QuerySearchResultProvider;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.settings.Settings;
import org.server.search.util.trove.ExtTIntArrayList;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TransportSearchDfsQueryThenFetchAction extends TransportSearchTypeAction {

    @Inject public TransportSearchDfsQueryThenFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                                          TransportSearchCache transportSearchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, indicesService, transportSearchCache, searchService, searchPhaseController);
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<DfsSearchResult> {

        /**
         * 存储分布式频率搜索（DfsSearchResult）的集合。
         * 这个集合包含的是在执行分布式搜索（DFS）阶段的结果，用于收集全局的词频信息，
         * 以便在后续的查询阶段更准确地对搜索结果进行评分。
         */
        private final Collection<DfsSearchResult> dfsResults = transportSearchCache.obtainDfsResults();

        /**
         * 存储查询搜索结果提供者（QuerySearchResultProvider）的映射，键为SearchShardTarget。
         * 这个映射包含了每个分片目标的查询搜索结果提供者，用于执行实际的查询操作，
         * 并返回只包含必要元数据的查询结果（不包括完整的文档内容）。
         */
        private final Map<SearchShardTarget, QuerySearchResultProvider> queryResults = transportSearchCache.obtainQueryResults();

        /**
         * 存储获取搜索结果（FetchSearchResult）的映射，键为SearchShardTarget。
         * 这个映射包含了每个分片目标的获取搜索结果，用于在查询阶段之后，
         * 根据查询结果的元数据去相应分片获取实际的文档内容。
         */
        private final Map<SearchShardTarget, FetchSearchResult> fetchResults = transportSearchCache.obtainFetchResults();
        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override protected void sendExecuteFirstPhase(Node node, InternalSearchRequest request, SearchServiceListener<DfsSearchResult> listener) {
            searchService.sendExecuteDfs(node, request, listener);
        }

        @Override protected void processFirstPhaseResult(ShardRouting shard, DfsSearchResult result) {
            dfsResults.add(result);
        }

        @Override
        protected void moveToSecondPhase() {
            // 聚合分布式频率搜索结果。
            final AggregatedDfs dfs = searchPhaseController.aggregateDfs(dfsResults);
            // 创建一个计数器，用于跟踪查询操作的执行情况。
            final AtomicInteger counter = new AtomicInteger(dfsResults.size());

            // 计算本地操作的数量。
            int localOperations = 0;
            for (DfsSearchResult dfsResult : dfsResults) {
                Node node = nodes.get(dfsResult.shardTarget().nodeId());
                // 判断分片是否在当前节点上。
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    // 对于非本地节点，创建查询搜索请求并执行。
                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                    executeQuery(counter, querySearchRequest, node);
                }
            }

            // 如果有本地操作，根据线程模型执行它们。
            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    // 如果使用单线程模型，在线程池中执行所有本地查询操作。
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            for (DfsSearchResult dfsResult : dfsResults) {
                                Node node = nodes.get(dfsResult.shardTarget().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                                    executeQuery(counter, querySearchRequest, node);
                                }
                            }
                            // 释放分布式频率搜索结果缓存。
                            transportSearchCache.releaseDfsResults(dfsResults);
                        }
                    });
                } else {
                    // 如果使用每个分片一个线程或无线程模型，为每个本地查询操作创建线程或直接执行。
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (DfsSearchResult dfsResult : dfsResults) {
                        final Node node = nodes.get(dfsResult.shardTarget().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                            if (localAsync) {
                                // 如果使用每个分片一个线程模型，在线程池中执行查询操作。
                                threadPool.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        executeQuery(counter, querySearchRequest, node);
                                    }
                                });
                            } else {
                                // 否则，直接执行查询操作。
                                executeQuery(counter, querySearchRequest, node);
                            }
                        }
                    }
                    // 释放分布式频率搜索结果缓存。
                    transportSearchCache.releaseDfsResults(dfsResults);
                }
            }
        }

        /**
         * 执行查询阶段的方法，对指定节点发送查询请求。
         *
         * @param counter 计数器，用于跟踪所有查询操作的完成情况。
         * @param querySearchRequest 查询请求对象，包含查询的详细信息。
         * @param node 目标节点，查询请求将发送到这个节点。
         */
        private void executeQuery(final AtomicInteger counter, QuerySearchRequest querySearchRequest, Node node) {
            // 使用searchService发送执行查询请求到指定节点。
            searchService.sendExecuteQuery(node, querySearchRequest, new SearchServiceListener<QuerySearchResult>() {
                @Override
                public void onResult(QuerySearchResult result) {
                    // 当查询操作成功完成时，将结果存储到queryResults映射中。
                    queryResults.put(result.shardTarget(), result);
                    // 如果当前是最后一个完成的查询操作，开始执行获取阶段。
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // 如果查询操作失败，记录调试信息。
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to execute query phase", t);
                    }
                    // 由于操作失败，减少成功操作的计数。
                    successulOps.decrementAndGet();
                    // 如果当前是最后一个完成的查询操作，开始执行获取阶段。
                    if (counter.decrementAndGet() == 0) {
                        executeFetchPhase();
                    }
                }
            });
        }

        /**
         * 执行搜索请求的获取阶段，根据查询结果获取文档内容。
         */
        private void executeFetchPhase() {
            // 对查询结果进行排序，准备进行获取操作。
            sortedShardList = searchPhaseController.sortDocs(queryResults.values());
            // 获取需要加载的文档ID列表。
            final Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad = searchPhaseController.docIdsToLoad(sortedShardList);

            // 如果没有文档需要加载，直接完成搜索请求。
            if (docIdsToLoad.isEmpty()) {
                finishHim();
            }

            // 创建计数器，用于跟踪所有获取操作的完成情况。
            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.size());
            int localOperations = 0;
            // 遍历需要加载的文档ID列表。
            for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                Node node = nodes.get(entry.getKey().nodeId());
                // 判断分片是否在当前节点上。
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    // 对于非本地节点，创建获取搜索请求并执行。
                    FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(entry.getKey()).id(), entry.getValue());
                    executeFetch(counter, fetchSearchRequest, node);
                }
            }

            // 如果有本地操作，根据线程模型执行它们。
            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    // 如果使用单线程模型，在线程池中执行所有本地获取操作。
                    threadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                                Node node = nodes.get(entry.getKey().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(entry.getKey()).id(), entry.getValue());
                                    executeFetch(counter, fetchSearchRequest, node);
                                }
                            }
                        }
                    });
                } else {
                    // 如果使用每个分片一个线程或无线程模型，为每个本地获取操作创建线程或直接执行。
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (Map.Entry<SearchShardTarget, ExtTIntArrayList> entry : docIdsToLoad.entrySet()) {
                        final Node node = nodes.get(entry.getKey().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(queryResults.get(entry.getKey()).id(), entry.getValue());
                            if (localAsync) {
                                // 如果使用每个分片一个线程模型，在线程池中执行获取操作。
                                threadPool.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        executeFetch(counter, fetchSearchRequest, node);
                                    }
                                });
                            } else {
                                // 否则，直接执行获取操作。
                                executeFetch(counter, fetchSearchRequest, node);
                            }
                        }
                    }
                }
            }
        }

        /**
         * 执行获取阶段的方法，对指定节点发送获取请求。
         *
         * @param counter 计数器，用于跟踪所有获取操作的完成情况。
         * @param fetchSearchRequest 获取请求对象，包含获取的详细信息。
         * @param node 目标节点，获取请求将发送到这个节点。
         */
        private void executeFetch(final AtomicInteger counter, FetchSearchRequest fetchSearchRequest, Node node) {
            // 使用searchService发送执行获取请求到指定节点。
            searchService.sendExecuteFetch(node, fetchSearchRequest, new SearchServiceListener<FetchSearchResult>() {
                @Override
                public void onResult(FetchSearchResult result) {
                    // 当获取操作成功完成时，将结果存储到fetchResults映射中。
                    fetchResults.put(result.shardTarget(), result);
                    // 如果当前是最后一个完成的获取操作，调用finishHim方法结束搜索请求。
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // 如果获取操作失败，记录调试信息。
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to execute fetch phase", t);
                    }
                    // 由于操作失败，减少成功操作的计数。
                    successulOps.decrementAndGet();
                    // 如果当前是最后一个完成的获取操作，调用finishHim方法结束搜索请求。
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }
            });
        }

        /**
         * 完成搜索请求的方法。
         */
        private void finishHim() {
            // 合并查询和获取的结果，创建内部搜索响应。
            final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryResults, fetchResults);
            // 如果请求包含滚动搜索参数，构建滚动ID。
            String scrollIdX = null;
            if (request.scroll() != null) {
                scrollIdX = TransportSearchHelper.buildScrollId(request.searchType(), fetchResults.values());
            }
            final String scrollId = scrollIdX;
            // 释放查询结果缓存。
            transportSearchCache.releaseQueryResults(queryResults);
            // 释放获取结果缓存。
            transportSearchCache.releaseFetchResults(fetchResults);
            // 根据请求是否需要线程化监听器，选择适当的方式调用监听器的onResponse方法。
            if (request.listenerThreaded()) {
                threadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get()));
                    }
                });
            } else {
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get()));
            }
        }
    }
}
