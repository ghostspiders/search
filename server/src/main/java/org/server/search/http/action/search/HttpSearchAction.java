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

package org.server.search.http.action.search;

import com.google.inject.Inject;
import org.server.search.SearchIllegalArgumentException;
import org.server.search.action.ActionListener;
import org.server.search.action.search.SearchOperationThreading;
import org.server.search.action.search.SearchRequest;
import org.server.search.action.search.SearchResponse;
import org.server.search.action.search.SearchType;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpActions;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.index.query.json.JsonQueryBuilders;
import org.server.search.index.query.json.QueryStringJsonQueryBuilder;
import org.server.search.search.Scroll;
import org.server.search.search.builder.SearchSourceBuilder;
import org.server.search.util.TimeValue;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static org.server.search.http.HttpResponse.Status.*;

 
public class HttpSearchAction extends BaseHttpServerHandler {

    public final static Pattern fieldsPattern;


    static {
        fieldsPattern = Pattern.compile(",");
    }

    @Inject public HttpSearchAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/_search", this);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/_search", this);
        httpService.registerHandler(HttpRequest.Method.GET, "/{index}/{type}/_search", this);
        httpService.registerHandler(HttpRequest.Method.POST, "/{index}/{type}/_search", this);
    }

    /**
     * 处理HTTP请求的方法。
     *
     * @param request HTTP请求对象，包含客户端发送的请求信息。
     * @param channel HTTP通道对象，用于发送响应。
     */
    @Override
    public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        SearchRequest searchRequest;
        try {
            // 解析HTTP请求，构建搜索请求对象。
            searchRequest = parseSearchRequest(request);
            // 设置搜索请求在非线程池中执行。
            searchRequest.listenerThreaded(false);
            // 根据请求参数获取搜索操作的线程模型。
            SearchOperationThreading operationThreading = SearchOperationThreading.fromString(
                    request.param("operationThreading"),
                    SearchOperationThreading.SINGLE_THREAD // 默认使用单线程模型
            );
            // 如果请求指定不使用线程(NO_THREADS)，则改为使用单线程(SINGLE_THREAD)。
            if (operationThreading == SearchOperationThreading.NO_THREADS) {
                operationThreading = SearchOperationThreading.SINGLE_THREAD;
            }
            // 设置搜索请求的线程模型。
            searchRequest.operationThreading(operationThreading);
        } catch (Exception e) {
            // 如果解析请求时发生异常，发送错误响应。
            try {
                channel.sendResponse(new JsonHttpResponse(request, BAD_REQUEST,
                        JsonBuilder.cached().startObject().field("error", e.getMessage()).endObject()));
            } catch (IOException e1) {
                // 如果发送响应失败，记录错误日志。
                logger.error("Failed to send failure response", e1);
            }
            return;
        }

        // 执行搜索请求。
        client.execSearch(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse result) {
                try {
                    // 搜索成功，构建JSON格式的响应体。
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject();
                    result.toJson(builder);
                    builder.endObject();
                    // 发送成功的JSON响应。
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    // 如果构建响应时发生异常，调用onFailure处理。
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    // 发送包含异常信息的错误响应。
                    channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                } catch (IOException e1) {
                    // 如果发送响应失败，记录错误日志。
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override public boolean spawn() {
        return false;
    }

    /**
     * 解析HTTP请求并构建搜索请求。
     *
     * @param request HTTP请求对象，包含搜索相关的查询参数。
     * @return 构建的搜索请求对象，用于执行搜索操作。
     */
    private SearchRequest parseSearchRequest(HttpRequest request) {
        // 从请求参数中获取索引名称，并分割成数组。
        String[] indices = HttpActions.splitIndices(request.param("index"));
        // 构建搜索请求对象，传入索引名称数组和搜索源。
        SearchRequest searchRequest = new SearchRequest(indices, parseSearchSource(request));

        // 获取搜索类型参数，并设置搜索请求的搜索类型。
        String searchType = request.param("searchType");
        if (searchType != null) {
            if ("dfs_query_then_fetch".equals(searchType)) {
                searchRequest.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            } else if ("dfs_query_and_fetch".equals(searchType)) {
                searchRequest.searchType(SearchType.DFS_QUERY_AND_FETCH);
            } else if ("query_then_fetch".equals(searchType)) {
                searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
            } else if ("query_and_fetch".equals(searchType)) {
                searchRequest.searchType(SearchType.QUERY_AND_FETCH);
            } else {
                // 如果搜索类型参数不是预定义的值，抛出异常。
                throw new SearchIllegalArgumentException("No search type for [" + searchType + "]");
            }
        } else {
            // 如果没有指定搜索类型，默认为QUERY_THEN_FETCH。
            searchRequest.searchType(SearchType.QUERY_THEN_FETCH);
        }

        // 获取from参数，并设置搜索请求的起始偏移量。
        String from = request.param("from");
        if (from != null) {
            searchRequest.from(Integer.parseInt(from));
        }

        // 获取size参数，并设置搜索请求的返回结果数量。
        String size = request.param("size");
        if (size != null) {
            searchRequest.size(Integer.parseInt(size));
        }

        // TODO: 这里可以处理每个查询的boost值。
        // searchRequest.queryBoost();

        // 获取scroll参数，并设置搜索请求的滚动时间。
        String scroll = request.param("scroll");
        if (scroll != null) {
            searchRequest.scroll(new Scroll(TimeValue.parseTimeValue(scroll, null)));
        }

        // 获取timeout参数，并设置搜索请求的超时时间。
        String timeout = request.param("timeout");
        if (timeout != null) {
            searchRequest.timeout(TimeValue.parseTimeValue(timeout, null));
        }

        // 获取type参数，并设置搜索请求的文档类型。
        String typesParam = request.param("type");
        if (typesParam != null) {
            searchRequest.types(HttpActions.splitTypes(typesParam));
        }

        // 设置查询提示，可能用于缓存或其他优化。
        searchRequest.queryHint(request.param("queryHint"));

        // 返回构建好的搜索请求对象。
        return searchRequest;
    }

    /**
     * 解析HTTP请求并构建搜索源。
     *
     * @param request HTTP请求对象，包含查询参数和可能的请求体。
     * @return 构建的搜索源对象，用于执行搜索操作。
     */
    private String parseSearchSource(HttpRequest request) {
        // 检查请求是否有内容，如果有，则直接返回请求体的内容。
        if (request.hasContent()) {
            return request.contentAsString();
        }

        // 从请求参数中获取查询字符串'q'。
        String queryString = request.param("q");
        if (queryString == null) {
            // 如果查询字符串不存在，抛出异常。
            throw new SearchIllegalArgumentException("No query to execute, not in body, and not bounded to 'q' parameter");
        }

        // 使用查询字符串构建查询构建器。
        QueryStringJsonQueryBuilder queryBuilder = JsonQueryBuilders.queryString(queryString);
        // 设置默认搜索字段。
        queryBuilder.defaultField(request.param("df"));
        // 设置分析器。
        queryBuilder.analyzer(request.param("analyzer"));

        // 获取并设置默认的操作符。
        String defaultOperator = request.param("defaultOperator");
        if (defaultOperator != null) {
            if ("OR".equals(defaultOperator)) {
                queryBuilder.defualtOperator(QueryStringJsonQueryBuilder.Operator.OR); // 修正方法名：defualtOperator 应为 defaultOperator
            } else if ("AND".equals(defaultOperator)) {
                queryBuilder.defualtOperator(QueryStringJsonQueryBuilder.Operator.AND);
            } else {
                // 如果默认操作符不是"OR"或"AND"，抛出异常。
                throw new SearchIllegalArgumentException("Unsupported defaultOperator [" + defaultOperator + "], can either be [OR] or [AND]");
            }
        }

        // TODO: 这里可以添加更多的查询参数处理。

        // 使用查询构建器构建搜索源构建器，并添加查询。
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);

        // 设置查询解析器名称。
        searchSourceBuilder.queryParserName(request.param("queryParserName"));
        // 设置是否需要解释查询。
        String explain = request.param("explain");
        if (explain != null) {
            searchSourceBuilder.explain(Boolean.parseBoolean(explain));
        }

        // 添加需要返回的字段。
        List<String> fields = request.params("field");
        if (fields != null && !fields.isEmpty()) {
            searchSourceBuilder.fields(fields);
        }
        // 添加单个字段。
        String sField = request.param("fields");
        if (sField != null) {
            // 这里假设fieldsPattern是一个用于分割字段的正则表达式。
            String[] sFields = fieldsPattern.split(sField);
            if (sFields != null) {
                for (String field : sFields) {
                    searchSourceBuilder.field(field);
                }
            }
        }

        // 添加排序字段。
        List<String> sorts = request.params("sort");
        if (sorts != null && !sorts.isEmpty()) {
            for (String sort : sorts) {
                // 处理包含排序方向的字段。
                int delimiter = sort.lastIndexOf(":");
                if (delimiter != -1) {
                    String sortField = sort.substring(0, delimiter);
                    String reverse = sort.substring(delimiter + 1);
                    searchSourceBuilder.sort(sortField, reverse.equals("reverse"));
                } else {
                    // 只包含排序字段，不包含方向。
                    searchSourceBuilder.sort(sort);
                }
            }
        }

        // TODO: 这里可以添加更多的源参数处理。

        // 构建并返回搜索源字符串。
        return searchSourceBuilder.build();
    }
}
