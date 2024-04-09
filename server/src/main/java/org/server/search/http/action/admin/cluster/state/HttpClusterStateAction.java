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

package org.server.search.http.action.admin.cluster.state;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.cluster.state.ClusterStateRequest;
import org.server.search.action.admin.cluster.state.ClusterStateResponse;
import org.server.search.client.Client;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.metadata.IndexMetaData;
import org.server.search.cluster.routing.IndexRoutingTable;
import org.server.search.cluster.routing.IndexShardRoutingTable;
import org.server.search.cluster.routing.RoutingNode;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpClusterStateAction extends BaseHttpServerHandler {

    @Inject public HttpClusterStateAction(Settings settings, HttpServer httpServer, Client client) {
        super(settings, client);

        httpServer.registerHandler(HttpRequest.Method.GET, "/_cluster/state", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        client.admin().cluster().execState(new ClusterStateRequest(), new ActionListener<ClusterStateResponse>() {
            @Override public void onResponse(ClusterStateResponse response) {
                try {
                    ClusterState state = response.state();
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();

                    // meta data
                    builder.startObject("metadata");
                    builder.field("maxNumberOfShardsPerNode", state.metaData().maxNumberOfShardsPerNode());
                    builder.startObject("indices");
                    for (IndexMetaData indexMetaData : state.metaData()) {
                        builder.startObject(indexMetaData.index());

                        builder.startObject("settings");
                        for (Map.Entry<String, String> entry : indexMetaData.settings().getAsMap().entrySet()) {
                            builder.startObject("setting").field("name", entry.getKey()).field("value", entry.getValue()).endObject();
                        }
                        builder.endObject();

                        builder.startObject("mappings");
                        for (Map.Entry<String, String> entry : indexMetaData.mappings().entrySet()) {
                            builder.startObject("mapping").field("name", entry.getKey()).field("value", entry.getValue()).endObject();
                        }
                        builder.endObject();

                        builder.endObject();
                    }
                    builder.endObject();
                    builder.endObject();

                    // routing table
                    builder.startObject("routingTable");
                    builder.startObject("indices");
                    for (IndexRoutingTable indexRoutingTable : state.routingTable()) {
                        builder.startObject(indexRoutingTable.index());
                        builder.startObject("shards");
                        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                            builder.startArray(Integer.toString(indexShardRoutingTable.shardId().id()));
                            for (ShardRouting shardRouting : indexShardRoutingTable) {
                                jsonShardRouting(builder, shardRouting);
                            }
                            builder.endArray();
                        }
                        builder.endObject();
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.endObject();

                    // routing nodes
                    builder.startObject("routingNodes");
                    builder.startArray("unassigned");
                    for (ShardRouting shardRouting : state.routingNodes().unassigned()) {
                        jsonShardRouting(builder, shardRouting);
                    }
                    builder.endArray();
                    builder.startObject("nodes");
                    for (RoutingNode routingNode : state.routingNodes()) {
                        builder.startArray(routingNode.nodeId());
                        for (ShardRouting shardRouting : routingNode) {
                            jsonShardRouting(builder, shardRouting);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                    builder.endObject();

                    builder.endObject();
                    channel.sendResponse(new JsonHttpResponse(request, HttpResponse.Status.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            private void jsonShardRouting(JsonBuilder builder, ShardRouting shardRouting) throws IOException {
                builder.startObject()
                        .field("state", shardRouting.state())
                        .field("primary", shardRouting.primary())
                        .field("nodeId", shardRouting.currentNodeId())
                        .field("relocatingNodeId", shardRouting.relocatingNodeId())
                        .field("shardId", shardRouting.shardId().id())
                        .field("index", shardRouting.shardId().index().name())
                        .endObject();
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