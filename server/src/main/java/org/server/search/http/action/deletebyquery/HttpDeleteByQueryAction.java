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

package org.server.search.http.action.deletebyquery;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.deletebyquery.DeleteByQueryRequest;
import org.server.search.action.deletebyquery.DeleteByQueryResponse;
import org.server.search.action.deletebyquery.IndexDeleteByQueryResponse;
import org.server.search.action.deletebyquery.ShardDeleteByQueryRequest;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpActions;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.TimeValue;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

import static org.server.search.http.HttpResponse.Status.*;
import static org.server.search.http.action.support.HttpActions.*;

 
public class HttpDeleteByQueryAction extends BaseHttpServerHandler {

    @Inject public HttpDeleteByQueryAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.DELETE, "/{index}/_query", this);
        httpService.registerHandler(HttpRequest.Method.DELETE, "/{index}/{type}/_query", this);
    }

    @Override
    public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        // 创建一个新的DeleteByQueryRequest对象，并使用请求参数中的索引名称进行初始化。
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(splitIndices(request.param("index")));
        // 设置请求不需要线程化的监听器。
        deleteByQueryRequest.listenerThreaded(false);
        try {
            // 解析请求体中的查询源，并设置到删除查询请求中。
            deleteByQueryRequest.querySource(HttpActions.parseQuerySource(request));
            // 设置查询解析器名称。
            deleteByQueryRequest.queryParserName(request.param("queryParserName"));
            // 获取请求参数中的类型参数，并设置到删除查询请求中。
            String typesParam = request.param("type");
            if (typesParam != null) {
                deleteByQueryRequest.types(HttpActions.splitTypes(typesParam));
            }
            // 解析请求参数中的超时时间，并设置到删除查询请求中。
            deleteByQueryRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), ShardDeleteByQueryRequest.DEFAULT_TIMEOUT));
        } catch (Exception e) {
            // 如果在设置删除查询请求时发生异常，发送错误响应。
            try {
                channel.sendResponse(new JsonHttpResponse(request, PRECONDITION_FAILED,
                        JsonBuilder.cached().startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                // 如果发送响应失败，记录错误日志。
                logger.error("Failed to send failure response", e1);
            }
            return;
        }
        // 执行删除查询请求。
        client.execDeleteByQuery(deleteByQueryRequest, new ActionListener<DeleteByQueryResponse>() {
            @Override
            public void onResponse(DeleteByQueryResponse result) {
                try {
                    // 构建JSON格式的成功响应。
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject().field("ok", true);

                    // 构建_indices节点，包含每个索引的删除查询结果。
                    builder.startObject("_indices");
                    for (IndexDeleteByQueryResponse indexDeleteByQueryResponse : result.indices().values()) {
                        builder.startObject(indexDeleteByQueryResponse.index());

                        // 构建_shards节点，包含分片的统计信息。
                        builder.startObject("_shards");
                        builder.field("total", indexDeleteByQueryResponse.totalShards());
                        builder.field("successful", indexDeleteByQueryResponse.successfulShards());
                        builder.field("failed", indexDeleteByQueryResponse.failedShards());
                        builder.endObject();

                        builder.endObject();
                    }
                    builder.endObject();

                    builder.endObject();
                    // 发送构建好的JSON响应。
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    // 如果构建响应时发生异常，调用onFailure处理。
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    // 如果执行删除查询请求失败，发送包含异常信息的错误响应。
                    channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                } catch (IOException e1) {
                    // 如果发送响应失败，记录错误日志。
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override public boolean spawn() {
        // we don't spawn since we fork in index replication based on operation
        return false;
    }
}