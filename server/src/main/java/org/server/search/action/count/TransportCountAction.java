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

package org.server.search.action.count;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.action.TransportActions;
import org.server.search.action.support.broadcast.TransportBroadcastOperationAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.index.shard.IndexShard;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import java.util.concurrent.atomic.AtomicReferenceArray;


public class TransportCountAction extends TransportBroadcastOperationAction<CountRequest, CountResponse, ShardCountRequest, ShardCountResponse> {

    // 构造函数，注入依赖的服务和设置
    @Inject
    public TransportCountAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                TransportService transportService, IndicesService indicesService) {
        // 调用父类的构造函数，传入依赖的服务和设置
        super(settings, threadPool, clusterService, transportService, indicesService);
    }

    // 重写方法，返回这个操作的动作名称
    @Override
    protected String transportAction() {
        // 返回动作名称为"COUNT"，表示这是一个计数操作
        return TransportActions.COUNT;
    }

    // 重写方法，返回这个操作在分片级别上的动作名称
    @Override
    protected String transportShardAction() {
        // 返回分片级别上的动作名称，用于在分片上执行计数操作
        return "indices/count/shard";
    }

    // 重写方法，创建一个新的计数请求对象
    @Override
    protected CountRequest newRequest() {
        // 返回一个新的CountRequest对象
        return new CountRequest();
    }

    // 重写方法，创建一个新的分片级别的计数请求对象
    @Override
    protected ShardCountRequest newShardRequest() {
        // 返回一个新的ShardCountRequest对象，用于在分片上执行计数请求
        return new ShardCountRequest();
    }

    @Override protected ShardCountRequest newShardRequest(ShardRouting shard, CountRequest request) {
        return new ShardCountRequest(shard.index(), shard.id(), request.querySource(), request.minScore(), request.queryParserName(), request.types());
    }

    @Override protected ShardCountResponse newShardResponse() {
        return new ShardCountResponse();
    }

    @Override
    protected CountResponse newResponse(CountRequest request, AtomicReferenceArray shardsResponses) {
        // 初始化成功和失败的分片计数器
        int successfulShards = 0;
        int failedShards = 0;
        // 初始化计数结果
        long count = 0;

        // 遍历所有分片的响应
        for (int i = 0; i < shardsResponses.length(); i++) {
            // 从AtomicReferenceArray中获取第i个分片的计数响应
            ShardCountResponse shardCountResponse = (ShardCountResponse) shardsResponses.get(i);
            // 如果分片响应为null，表示请求失败，增加失败分片计数器
            if (shardCountResponse == null) {
                failedShards++;
            } else {
                // 否则，累加分片的计数结果到总数中，并增加成功分片计数器
                count += shardCountResponse.count();
                successfulShards++;
            }
        }

        // 创建并返回一个新的CountResponse对象，包含总数、成功和失败的分片数
        return new CountResponse(count, successfulShards, failedShards);
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    @Override
    protected ShardCountResponse shardOperation(ShardCountRequest request) throws SearchException {
        // 根据请求中的索引名称和分片ID获取IndexShard实例
        IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.shardId());

        // 使用IndexShard实例执行计数操作，传入请求中的最小分数、查询源、查询解析器名称和类型
        long count = indexShard.count(
                request.minScore(),
                request.querySource(),
                request.queryParserName(),
                request.types()
        );

        // 创建并返回一个新的ShardCountResponse对象，包含索引名称、分片ID和计数结果
        return new ShardCountResponse(request.index(), request.shardId(), count);
    }
}
