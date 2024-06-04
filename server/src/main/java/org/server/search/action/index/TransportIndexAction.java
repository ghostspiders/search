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

package org.server.search.action.index;

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
import org.server.search.util.UUID;
import org.server.search.util.settings.Settings;

public class TransportIndexAction extends TransportShardReplicationOperationAction<IndexRequest, IndexResponse> {

    // 是否自动创建索引，如果请求的索引不存在且此标志为true，则会自动创建
    private final boolean autoCreateIndex;
    // 是否允许系统生成文档ID
    private final boolean allowIdGeneration;
    // 用于创建索引的内部操作
    private final TransportCreateIndexAction createIndexAction;

    // 构造函数，注入相关的服务和组件设置
    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                TransportCreateIndexAction createIndexAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.createIndexAction = createIndexAction;
        // 初始化设置，如果配置中没有明确指定，则默认为true
        this.autoCreateIndex = componentSettings.getAsBoolean("autoCreateIndex", true);
        this.allowIdGeneration = componentSettings.getAsBoolean("allowIdGeneration", true);
    }

    // 执行索引操作
    @Override
    protected void doExecute(final IndexRequest indexRequest, final ActionListener<IndexResponse> listener) {
        // 如果允许ID生成，并且请求中没有指定ID，则生成一个随机UUID作为ID
        // 并且将操作类型改为创建（CREATE）
        if (allowIdGeneration) {
            if (indexRequest.id() == null) {
                indexRequest.id(UUID.randomUUID().toString());
                indexRequest.opType(IndexRequest.OpType.CREATE);
            }
        }
        // 如果自动创建索引标志为true，并且集群状态中的元数据没有包含请求的索引
        if (autoCreateIndex) {
            if (!clusterService.state().metaData().hasIndex(indexRequest.index())) {
                // 执行创建索引操作
                createIndexAction.execute(new CreateIndexRequest(indexRequest.index()), new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse result) {
                        // 创建索引成功后执行索引文档操作
                        TransportIndexAction.super.doExecute(indexRequest, listener);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        // 如果异常是因为索引已存在，则继续执行索引文档操作
                        if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                            TransportIndexAction.super.doExecute(indexRequest, listener);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                // 如果索引已存在，则直接执行索引文档操作
                super.doExecute(indexRequest, listener);
            }
        }
    }

    // 创建新的IndexRequest实例
    @Override
    protected IndexRequest newRequestInstance() {
        return new IndexRequest();
    }

    // 创建新的IndexResponse实例
    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    // 返回传输层操作的名称
    @Override
    protected String transportAction() {
        return TransportActions.INDEX;
    }

    // 获取涉及的分片迭代器
    @Override
    protected ShardsIterator shards(IndexRequest request) {
        return indicesService.indexServiceSafe(request.index()).operationRouting()
                .indexShards(clusterService.state(), request.type(), request.id());
    }

    // 在主分片上执行操作
    @Override
    protected IndexResponse shardOperationOnPrimary(ShardOperationRequest shardRequest) {
        IndexRequest request = shardRequest.request;
        // 根据操作类型执行索引或创建操作
        if (request.opType() == IndexRequest.OpType.INDEX) {
            indexShard(shardRequest).index(request.type(), request.id(), request.source());
        } else {
            indexShard(shardRequest).create(request.type(), request.id(), request.source());
        }
        return new IndexResponse(request.index(), request.type(), request.id());
    }

    // 在备份分片上执行操作
    @Override
    protected void shardOperationOnBackup(ShardOperationRequest shardRequest) {
        IndexRequest request = shardRequest.request;
        // 备份分片上执行相同的索引或创建操作
        if (request.opType() == IndexRequest.OpType.INDEX) {
            indexShard(shardRequest).index(request.type(), request.id(), request.source());
        } else {
            indexShard(shardRequest).create(request.type(), request.id(), request.source());
        }
    }
}