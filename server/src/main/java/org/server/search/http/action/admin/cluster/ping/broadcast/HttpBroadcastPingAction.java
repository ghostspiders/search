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

package org.server.search.http.action.admin.cluster.ping.broadcast;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.server.search.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.server.search.action.support.broadcast.BroadcastOperationThreading;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpActions;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

import static org.server.search.http.HttpResponse.Status.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpBroadcastPingAction extends BaseHttpServerHandler {

    @Inject public HttpBroadcastPingAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/_ping/broadcast", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/_cluster/{index}/_ping/broadcast", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        BroadcastPingRequest broadcastPingRequest = new BroadcastPingRequest(HttpActions.splitIndices(request.param("index")));
        broadcastPingRequest.queryHint(request.param("queryHint"));
        BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(request.param("operationThreading"), BroadcastOperationThreading.SINGLE_THREAD);
        if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
            // since we don't spawn, don't allow no_threads, but change it to a single thread
            operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
        }
        broadcastPingRequest.operationThreading(operationThreading);
        client.admin().cluster().execPing(broadcastPingRequest, new ActionListener<BroadcastPingResponse>() {
            @Override public void onResponse(BroadcastPingResponse result) {
                try {
                    JsonBuilder generator = HttpJsonBuilder.cached(request);
                    generator.startObject()
                            .field("ok", true)
                            .field("totalShards", result.totalShards())
                            .field("successfulShards", result.successfulShards())
                            .field("failedShards", result.failedShards())
                            .endObject();
                    channel.sendResponse(new JsonHttpResponse(request, OK, generator));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override public boolean spawn() {
        return false;
    }
}