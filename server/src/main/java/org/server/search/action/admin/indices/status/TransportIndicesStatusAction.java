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

package org.server.search.action.admin.indices.status;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.action.TransportActions;
import org.server.search.action.support.shards.ShardOperationRequest;
import org.server.search.action.support.shards.TransportShardsOperationActions;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.index.engine.Engine;
import org.server.search.index.shard.InternalIndexShard;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.*;


public class TransportIndicesStatusAction extends TransportShardsOperationActions<IndicesStatusRequest, IndicesStatusResponse, TransportIndicesStatusAction.IndexShardStatusRequest, ShardStatus> {

    @Inject public TransportIndicesStatusAction(Settings settings, ClusterService clusterService, TransportService transportService, IndicesService indicesService, ThreadPool threadPool) {
        super(settings, clusterService, transportService, indicesService, threadPool);
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.STATUS;
    }

    @Override protected String transportShardAction() {
        return "indices/status/shard";
    }

    @Override protected IndicesStatusRequest newRequest() {
        return new IndicesStatusRequest();
    }

    @Override protected IndexShardStatusRequest newShardRequest() {
        return new IndexShardStatusRequest();
    }

    @Override protected IndexShardStatusRequest newShardRequest(ShardRouting shard, IndicesStatusRequest request) {
        return new IndexShardStatusRequest(shard.index(), shard.id());
    }

    @Override protected ShardStatus newShardResponse() {
        return new ShardStatus();
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override protected IndicesStatusResponse newResponse(IndicesStatusRequest request, ClusterState clusterState, AtomicReferenceArray<Object> shardsResponses) {
        final List<ShardStatus> shards = newArrayList();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object resp = shardsResponses.get(i);
            if (resp instanceof ShardStatus) {
                shards.add((ShardStatus) resp);
            }
        }
        return new IndicesStatusResponse(shards.toArray(new ShardStatus[shards.size()]), clusterState);
    }

    @Override protected ShardStatus shardOperation(IndexShardStatusRequest request) throws SearchException {
        InternalIndexShard indexShard = (InternalIndexShard) indicesService.indexServiceSafe(request.index()).shard(request.shardId());
        ShardStatus shardStatus = new ShardStatus(indexShard.routingEntry());
        shardStatus.state = indexShard.state();
        try {
            shardStatus.storeSize = indexShard.store().estimateSize();
        } catch (IOException e) {
            // failure to get the store size...
        }
        shardStatus.estimatedFlushableMemorySize = indexShard.estimateFlushableMemorySize();
        shardStatus.translogId = indexShard.translog().currentId();
        shardStatus.translogOperations = indexShard.translog().size();
        Engine.Searcher searcher = indexShard.searcher();
        try {
            shardStatus.docs = new ShardStatus.Docs();
            shardStatus.docs.numDocs = searcher.reader().numDocs();
            shardStatus.docs.maxDoc = searcher.reader().maxDoc();
            shardStatus.docs.deletedDocs = searcher.reader().numDeletedDocs();
        } finally {
            searcher.release();
        }
        return shardStatus;
    }

    public static class IndexShardStatusRequest extends ShardOperationRequest {

        IndexShardStatusRequest() {
        }

        IndexShardStatusRequest(String index, int shardId) {
            super(index, shardId);
        }
    }
}
