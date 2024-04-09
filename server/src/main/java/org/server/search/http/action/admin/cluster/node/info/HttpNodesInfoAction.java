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

package org.server.search.http.action.admin.cluster.node.info;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.cluster.node.info.NodeInfo;
import org.server.search.action.admin.cluster.node.info.NodesInfoRequest;
import org.server.search.action.admin.cluster.node.info.NodesInfoResponse;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpActions;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpNodesInfoAction extends BaseHttpServerHandler {

    @Inject public HttpNodesInfoAction(Settings settings, HttpServer httpServer, Client client) {
        super(settings, client);

        httpServer.registerHandler(HttpRequest.Method.GET, "/_cluster/nodes", this);
        httpServer.registerHandler(HttpRequest.Method.GET, "/_cluster/nodes/${nodeId}", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        String[] nodesIds = HttpActions.splitNodes(request.param("nodeId"));
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodesIds);
        nodesInfoRequest.listenerThreaded(false);
        client.admin().cluster().execNodesInfo(nodesInfoRequest, new ActionListener<NodesInfoResponse>() {
            @Override public void onResponse(NodesInfoResponse result) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();
                    builder.field("clusterName", result.clusterName().value());
                    for (NodeInfo nodeInfo : result) {
                        builder.startObject(nodeInfo.node().id());

                        builder.field("name", nodeInfo.node().name());
                        builder.field("transportAddress", nodeInfo.node().address().toString());
                        builder.field("dataNode", nodeInfo.node().dataNode());

                        builder.endObject();
                    }
                    builder.endObject();
                    channel.sendResponse(new JsonHttpResponse(request, HttpResponse.Status.OK, builder));
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
