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

package org.server.search.action.search;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.TransportActions;
import org.server.search.action.search.type.TransportSearchDfsQueryAndFetchAction;
import org.server.search.action.search.type.TransportSearchDfsQueryThenFetchAction;
import org.server.search.action.search.type.TransportSearchQueryAndFetchAction;
import org.server.search.action.search.type.TransportSearchQueryThenFetchAction;
import org.server.search.action.support.BaseAction;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import static org.server.search.action.search.SearchType.*;

 
public class TransportSearchAction extends BaseAction<SearchRequest, SearchResponse> {

    private final TransportSearchDfsQueryThenFetchAction dfsQueryThenFetchAction;

    private final TransportSearchQueryThenFetchAction queryThenFetchAction;

    private final TransportSearchDfsQueryAndFetchAction dfsQueryAndFetchAction;

    private final TransportSearchQueryAndFetchAction queryAndFetchAction;

    @Inject public TransportSearchAction(Settings settings, TransportService transportService,
                                         TransportSearchDfsQueryThenFetchAction dfsQueryThenFetchAction,
                                         TransportSearchQueryThenFetchAction queryThenFetchAction,
                                         TransportSearchDfsQueryAndFetchAction dfsQueryAndFetchAction,
                                         TransportSearchQueryAndFetchAction queryAndFetchAction) {
        super(settings);
        this.dfsQueryThenFetchAction = dfsQueryThenFetchAction;
        this.queryThenFetchAction = queryThenFetchAction;
        this.dfsQueryAndFetchAction = dfsQueryAndFetchAction;
        this.queryAndFetchAction = queryAndFetchAction;

        transportService.registerHandler(TransportActions.SEARCH, new TransportHandler());
    }

    @Override protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        if (searchRequest.searchType() == DFS_QUERY_THEN_FETCH) {
            dfsQueryThenFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.QUERY_THEN_FETCH) {
            queryThenFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.DFS_QUERY_AND_FETCH) {
            dfsQueryAndFetchAction.execute(searchRequest, listener);
        } else if (searchRequest.searchType() == SearchType.QUERY_AND_FETCH) {
            queryAndFetchAction.execute(searchRequest, listener);
        }
    }

    /**
     * 内部类，继承自BaseTransportRequestHandler，用于处理搜索请求。
     */
    private class TransportHandler extends BaseTransportRequestHandler<SearchRequest> {

        /**
         * 实现基类方法，创建一个新的搜索请求实例。
         * @return 新的搜索请求实例。
         */
        @Override
        public SearchRequest newInstance() {
            return new SearchRequest();
        }

        /**
         * 当接收到搜索请求时调用的方法。
         * @param request 接收到的搜索请求。
         * @param channel 传输通道，用于发送响应或异常。
         * @throws Exception 可能抛出的异常。
         */
        @Override
        public void messageReceived(SearchRequest request, final TransportChannel channel) throws Exception {
            // 设置请求不需要线程化的监听器。
            request.listenerThreaded(false);
            // 如果请求的操作线程模型是无线程(NO_THREADS)，则改为单线程(SINGLE_THREAD)。
            if (request.operationThreading() == SearchOperationThreading.NO_THREADS) {
                request.operationThreading(SearchOperationThreading.SINGLE_THREAD);
            }
            // 执行搜索请求，并使用ActionListener来处理响应或失败。
            execute(request, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse result) {
                    try {
                        // 如果搜索成功，通过通道发送响应。
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        // 如果发送响应时出现异常，调用onFailure方法。
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        // 如果执行搜索请求失败，通过通道发送异常。
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        // 如果发送异常响应时再次出现异常，记录警告日志。
                        logger.warn("Failed to send response for search", e1);
                    }
                }
            });
        }

        /**
         * 实现基类方法，指示是否为请求生成新线程。
         * @return 始终返回false，表示不生成新线程。
         */
        @Override
        public boolean spawn() {
            return false;
        }
    }
}
