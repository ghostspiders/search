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

package org.server.search.action.delete;

import com.google.inject.Inject;
import org.server.search.ExceptionsHelper;
import org.server.search.action.ActionListener;
import org.server.search.action.TransportActions;
import org.server.search.action.admin.indices.create.CreateIndexRequest;
import org.server.search.action.admin.indices.create.CreateIndexResponse;
import org.server.search.action.admin.indices.create.TransportCreateIndexAction;
import org.server.search.action.support.replication.TransportShardReplicationOperationAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.action.shard.ShardStateAction;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.indices.IndexAlreadyExistsException;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

 
public class TransportDeleteAction extends TransportShardReplicationOperationAction<DeleteRequest, DeleteResponse> {

    private final boolean autoCreateIndex;

    private final TransportCreateIndexAction createIndexAction;

    @Inject public TransportDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                         TransportCreateIndexAction createIndexAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = componentSettings.getAsBoolean("autoCreateIndex", true);
    }

    @Override protected void doExecute(final DeleteRequest deleteRequest, final ActionListener<DeleteResponse> listener) {
        if (autoCreateIndex) {
            if (!clusterService.state().metaData().hasIndex(deleteRequest.index())) {
                createIndexAction.execute(new CreateIndexRequest(deleteRequest.index()), new ActionListener<CreateIndexResponse>() {
                    @Override public void onResponse(CreateIndexResponse result) {
                        TransportDeleteAction.super.doExecute(deleteRequest, listener);
                    }

                    @Override public void onFailure(Throwable e) {
                        if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                            // we have the index, do it
                            TransportDeleteAction.super.doExecute(deleteRequest, listener);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                super.doExecute(deleteRequest, listener);
            }
        }
    }

    @Override protected DeleteRequest newRequestInstance() {
        return new DeleteRequest();
    }

    @Override protected DeleteResponse newResponseInstance() {
        return new DeleteResponse();
    }

    @Override protected String transportAction() {
        return TransportActions.DELETE;
    }

    @Override protected DeleteResponse shardOperationOnPrimary(ShardOperationRequest shardRequest) {
        DeleteRequest request = shardRequest.request;
        indexShard(shardRequest).delete(request.type(), request.id());
        return new DeleteResponse(request.index(), request.type(), request.id());
    }

    @Override protected void shardOperationOnBackup(ShardOperationRequest shardRequest) {
        DeleteRequest request = shardRequest.request;
        indexShard(shardRequest).delete(request.type(), request.id());
    }

    @Override protected ShardsIterator shards(DeleteRequest request) {
        return indicesService.indexServiceSafe(request.index()).operationRouting()
                .deleteShards(clusterService.state(), request.type(), request.id());
    }
}
