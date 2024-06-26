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

package org.server.search.http.action.admin.cluster.ping.single;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.cluster.ping.single.SinglePingRequest;
import org.server.search.action.admin.cluster.ping.single.SinglePingResponse;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

import static org.server.search.http.HttpResponse.Status.*;

 
public class HttpSinglePingAction extends BaseHttpServerHandler {

    @Inject public HttpSinglePingAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/{type}/{id}/_ping", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/_cluster/{index}/{type}/{id}/_ping", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        SinglePingRequest singlePingRequest = new SinglePingRequest(request.param("index"), request.param("type"), request.param("id"));
        // no need to have a threaded listener since we just send back a response
        singlePingRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't spawn
        singlePingRequest.threadedOperation(true);
        client.admin().cluster().execPing(singlePingRequest, new ActionListener<SinglePingResponse>() {
            @Override public void onResponse(SinglePingResponse result) {
                try {
                    JsonBuilder generator = HttpJsonBuilder.cached(request);
                    generator.startObject().field("ok", true).endObject();
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