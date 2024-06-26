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
import org.server.search.search.fetch.QueryFetchSearchResult;
import org.server.search.search.internal.InternalSearchRequest;
import org.server.search.search.internal.InternalSearchResponse;
import org.server.search.search.query.QuerySearchRequest;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.settings.Settings;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.server.search.action.search.type.TransportSearchHelper.*;

public class TransportSearchDfsQueryAndFetchAction extends TransportSearchTypeAction {

    @Inject public TransportSearchDfsQueryAndFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService,
                                                         TransportSearchCache transportSearchCache, SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, indicesService, transportSearchCache, searchService, searchPhaseController);
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<DfsSearchResult> {

        private final Collection<DfsSearchResult> dfsResults = transportSearchCache.obtainDfsResults();

        private final Map<SearchShardTarget, QueryFetchSearchResult> queryFetchResults = transportSearchCache.obtainQueryFetchResults();


        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
        }

        @Override protected void sendExecuteFirstPhase(Node node, InternalSearchRequest request, SearchServiceListener<DfsSearchResult> listener) {
            searchService.sendExecuteDfs(node, request, listener);
        }

        @Override protected void processFirstPhaseResult(ShardRouting shard, DfsSearchResult result) {
            dfsResults.add(result);
        }

        @Override protected void moveToSecondPhase() {
            final AggregatedDfs dfs = searchPhaseController.aggregateDfs(dfsResults);
            final AtomicInteger counter = new AtomicInteger(dfsResults.size());

            int localOperations = 0;
            for (DfsSearchResult dfsResult : dfsResults) {
                Node node = nodes.get(dfsResult.shardTarget().nodeId());
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                    executeSecondPhase(counter, node, querySearchRequest);
                }
            }
            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            for (DfsSearchResult dfsResult : dfsResults) {
                                Node node = nodes.get(dfsResult.shardTarget().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                                    executeSecondPhase(counter, node, querySearchRequest);
                                }
                            }
                            transportSearchCache.releaseDfsResults(dfsResults);
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (DfsSearchResult dfsResult : dfsResults) {
                        final Node node = nodes.get(dfsResult.shardTarget().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final QuerySearchRequest querySearchRequest = new QuerySearchRequest(dfsResult.id(), dfs);
                            if (localAsync) {
                                threadPool.execute(new Runnable() {
                                    @Override public void run() {
                                        executeSecondPhase(counter, node, querySearchRequest);
                                    }
                                });
                            } else {
                                executeSecondPhase(counter, node, querySearchRequest);
                            }
                        }
                    }
                    transportSearchCache.releaseDfsResults(dfsResults);
                }
            }
        }

        private void executeSecondPhase(final AtomicInteger counter, Node node, QuerySearchRequest querySearchRequest) {
            searchService.sendExecuteFetch(node, querySearchRequest, new SearchServiceListener<QueryFetchSearchResult>() {
                @Override public void onResult(QueryFetchSearchResult result) {
                    queryFetchResults.put(result.shardTarget(), result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to execute query phase", t);
                    }
                    successulOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }
            });
        }

        private void finishHim() {
            sortedShardList = searchPhaseController.sortDocs(queryFetchResults.values());
            final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, queryFetchResults, queryFetchResults);
            String scrollIdX = null;
            if (request.scroll() != null) {
                scrollIdX = buildScrollId(request.searchType(), queryFetchResults.values());
            }
            final String scrollId = scrollIdX;
            transportSearchCache.releaseQueryFetchResults(queryFetchResults);
            if (request.listenerThreaded()) {
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get()));
                    }
                });
            } else {
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get()));
            }
        }
    }
}