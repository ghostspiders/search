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

package org.server.search.action.admin.cluster.ping.broadcast;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.action.TransportActions;
import org.server.search.action.support.broadcast.TransportBroadcastOperationAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import java.util.concurrent.atomic.AtomicReferenceArray;


public class TransportBroadcastPingAction extends TransportBroadcastOperationAction<BroadcastPingRequest, BroadcastPingResponse, BroadcastShardPingRequest, BroadcastShardPingResponse> {

    @Inject public TransportBroadcastPingAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService, indicesService);
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Cluster.Ping.BROADCAST;
    }

    @Override protected String transportShardAction() {
        return "/cluster/ping/broadcast/shard";
    }

    @Override protected BroadcastPingRequest newRequest() {
        return new BroadcastPingRequest();
    }

    @Override protected BroadcastPingResponse newResponse(BroadcastPingRequest broadcastPingRequest, AtomicReferenceArray shardsResponses) {
        int successfulShards = 0;
        int failedShards = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            if (shardsResponses.get(i) == null) {
                failedShards++;
            } else {
                successfulShards++;
            }
        }
        return new BroadcastPingResponse(successfulShards, failedShards);
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected BroadcastShardPingRequest newShardRequest() {
        return new BroadcastShardPingRequest();
    }

    @Override protected BroadcastShardPingRequest newShardRequest(ShardRouting shard, BroadcastPingRequest request) {
        return new BroadcastShardPingRequest(shard.index(), shard.id());
    }

    @Override protected BroadcastShardPingResponse newShardResponse() {
        return new BroadcastShardPingResponse();
    }

    @Override protected BroadcastShardPingResponse shardOperation(BroadcastShardPingRequest broadcastShardPingRequest) throws SearchException {
        return new BroadcastShardPingResponse();
    }
}