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
import org.server.search.SearchIllegalArgumentException;
import org.server.search.action.ActionListener;
import org.server.search.action.TransportActions;
import org.server.search.action.search.type.ParsedScrollId;
import org.server.search.action.search.type.TransportSearchScrollQueryThenFetchAction;
import org.server.search.action.support.BaseAction;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import static org.server.search.action.search.type.ParsedScrollId.*;
import static org.server.search.action.search.type.TransportSearchHelper.*;

 
public class TransportSearchScrollAction extends BaseAction<SearchScrollRequest, SearchResponse> {

    private final TransportSearchScrollQueryThenFetchAction queryThenFetchAction;

    @Inject public TransportSearchScrollAction(Settings settings, TransportService transportService,
                                               TransportSearchScrollQueryThenFetchAction queryThenFetchAction) {
        super(settings);
        this.queryThenFetchAction = queryThenFetchAction;

        transportService.registerHandler(TransportActions.SEARCH_SCROLL, new TransportHandler());
    }

    @Override protected void doExecute(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        try {
            ParsedScrollId scrollId = parseScrollId(request.scrollId());
            if (scrollId.type().equals(QUERY_THEN_FETCH_TYPE)) {
                queryThenFetchAction.execute(request, scrollId, listener);
            } else {
                throw new SearchIllegalArgumentException("Scroll id type [" + scrollId.type() + "] unrecongnized");
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<SearchScrollRequest> {

        @Override public SearchScrollRequest newInstance() {
            return new SearchScrollRequest();
        }

        @Override public void messageReceived(SearchScrollRequest request, final TransportChannel channel) throws Exception {
            execute(request, new ActionListener<SearchResponse>() {
                @Override public void onResponse(SearchResponse result) {
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
                        logger.warn("Failed to send response for search", e1);
                    }
                }
            });
        }
    }
}