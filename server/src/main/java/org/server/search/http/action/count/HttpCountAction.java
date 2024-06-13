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

package org.server.search.http.action.count;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.count.CountRequest;
import org.server.search.action.count.CountResponse;
import org.server.search.action.support.broadcast.BroadcastOperationThreading;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpActions;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

import static org.server.search.action.count.CountRequest.*;
import static org.server.search.http.HttpResponse.Status.*;
import static org.server.search.http.action.support.HttpActions.*;

 
public class HttpCountAction extends BaseHttpServerHandler {

    @Inject public HttpCountAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/_count", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/_count", this);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/{type}/_count", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/{type}/_count", this);
    }

    @Override
    public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        // 创建一个计数请求对象
        CountRequest countRequest = new CountRequest(HttpActions.splitIndices(request.param("index")));
        // 设置为非线程监听模式，即直接在当前线程处理请求
        countRequest.listenerThreaded(false);

        try {
            // 从请求中获取操作线程模式，并转换为相应的枚举值
            BroadcastOperationThreading operationThreading = BroadcastOperationThreading.fromString(
                    request.param("operationThreading"), BroadcastOperationThreading.SINGLE_THREAD
            );
            // 如果请求中指定不使用线程（即NO_THREADS），则改为单线程模式
            if (operationThreading == BroadcastOperationThreading.NO_THREADS) {
                operationThreading = BroadcastOperationThreading.SINGLE_THREAD;
            }
            countRequest.operationThreading(operationThreading);

            // 设置计数请求的查询源
            countRequest.querySource(HttpActions.parseQuerySource(request));
            // 设置查询解析器名称
            countRequest.queryParserName(request.param("queryParserName"));
            // 设置查询提示
            countRequest.queryHint(request.param("queryHint"));
            // 设置最小分数限制
            countRequest.minScore(HttpActions.paramAsFloat(request.param("minScore"), DEFAULT_MIN_SCORE));

            // 如果请求中包含类型参数，则分割并设置到计数请求中
            String typesParam = request.param("type");
            if (typesParam != null) {
                countRequest.types(splitTypes(typesParam));
            }
        } catch (Exception e) {
            // 捕获异常并发送错误响应
            try {
                channel.sendResponse(new JsonHttpResponse(request, BAD_REQUEST,
                        JsonBuilder.cached().startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        // 执行计数请求，并设置响应处理器
        client.execCount(countRequest, new ActionListener<CountResponse>() {
            @Override
            public void onResponse(CountResponse response) {
                try {
                    // 构建并发送成功的JSON响应
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();
                    builder.field("count", response.count());
                    builder.startObject("_shards");
                    builder.field("total", response.totalShards());
                    builder.field("successful", response.successfulShards());
                    builder.field("failed", response.failedShards());
                    builder.endObject();
                    builder.endObject();
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    // 捕获异常并调用onFailure方法
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    // 发送包含异常信息的响应
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