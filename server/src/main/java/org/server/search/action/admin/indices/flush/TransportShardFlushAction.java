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

package org.server.search.action.admin.indices.flush;

import com.google.inject.Inject;
import org.server.search.action.support.replication.TransportShardReplicationOperationAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.action.shard.ShardStateAction;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;


public class TransportShardFlushAction extends TransportShardReplicationOperationAction<ShardFlushRequest, ShardFlushResponse> {

    @Inject public TransportShardFlushAction(Settings settings, TransportService transportService,
                                             ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                             ShardStateAction shardStateAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
    }

    @Override protected ShardFlushRequest newRequestInstance() {
        return new ShardFlushRequest();
    }

    @Override protected ShardFlushResponse newResponseInstance() {
        return new ShardFlushResponse();
    }

    @Override protected String transportAction() {
        return "indices/index/shard/flush";
    }

    @Override protected ShardFlushResponse shardOperationOnPrimary(ShardOperationRequest shardRequest) {
        ShardFlushRequest request = shardRequest.request;
        indexShard(shardRequest).flush();
        return new ShardFlushResponse();
    }

    @Override protected void shardOperationOnBackup(ShardOperationRequest shardRequest) {
        ShardFlushRequest request = shardRequest.request;
        indexShard(shardRequest).flush();
    }

    @Override protected ShardsIterator shards(ShardFlushRequest request) {
        return clusterService.state().routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }
}