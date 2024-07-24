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

package org.server.search.search;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.lucene.search.TopDocs;
import org.server.search.SearchException;
import org.server.search.cluster.ClusterService;
import org.server.search.index.IndexService;
import org.server.search.index.engine.Engine;
import org.server.search.index.shard.IndexShard;
import org.server.search.indices.IndicesService;
import org.server.search.search.dfs.CachedDfSource;
import org.server.search.search.dfs.DfsPhase;
import org.server.search.search.dfs.DfsSearchResult;
import org.server.search.search.fetch.FetchPhase;
import org.server.search.search.fetch.FetchSearchRequest;
import org.server.search.search.fetch.FetchSearchResult;
import org.server.search.search.fetch.QueryFetchSearchResult;
import org.server.search.search.internal.InternalScrollSearchRequest;
import org.server.search.search.internal.InternalSearchRequest;
import org.server.search.search.internal.SearchContext;
import org.server.search.search.query.QueryPhase;
import org.server.search.search.query.QuerySearchRequest;
import org.server.search.search.query.QuerySearchResult;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.concurrent.highscalelib.NonBlockingHashMapLong;
import org.server.search.util.io.FastStringReader;
import org.server.search.util.json.Jackson;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

 
public class SearchService extends AbstractComponent implements LifecycleComponent<SearchService> {

    // 生命周期管理器，用于处理组件的启动和关闭等生命周期操作
    private final Lifecycle lifecycle = new Lifecycle();

    // JSON工厂，用于创建JSON解析器和生成器，这里用于处理JSON数据
    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    // 集群服务，用于管理Elasticsearch集群的状态和节点间通信
    private final ClusterService clusterService;

    // 索引服务，提供对索引的管理，包括创建、删除和获取索引
    private final IndicesService indicesService;

    // 分布式频率筛选（DfsPhase）是执行搜索查询的第一阶段，用于收集文档频率信息
    private final DfsPhase dfsPhase;

    // 查询阶段，是执行搜索查询的第二阶段，用于处理查询请求
    private final QueryPhase queryPhase;

    // 获取阶段，是执行搜索查询的第三阶段，用于获取查询结果中的数据
    private final FetchPhase fetchPhase;

    // 原子长整型生成器，用于生成递增的ID值，保证ID的唯一性
    private final AtomicLong idGenerator = new AtomicLong();

    // 非阻塞的长整型哈希表，用于存储活动的搜索上下文（SearchContext）
    private final NonBlockingHashMapLong<SearchContext> activeContexts = new NonBlockingHashMapLong<SearchContext>();

    // 不可变的映射，存储字符串到搜索解析元素（SearchParseElement）的映射，用于解析搜索请求的不同部分
    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject public SearchService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                                 DfsPhase dfsPhase, QueryPhase queryPhase, FetchPhase fetchPhase) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.dfsPhase = dfsPhase;
        this.queryPhase = queryPhase;
        this.fetchPhase = fetchPhase;

        Map<String, SearchParseElement> elementParsers = new HashMap<String, SearchParseElement>();
        elementParsers.putAll(dfsPhase.parseElements());
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.putAll(fetchPhase.parseElements());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public SearchService start() throws SearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        return this;
    }

    @Override public SearchService stop() throws SearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        for (SearchContext context : activeContexts.values()) {
            freeContext(context);
        }
        activeContexts.clear();
        return this;
    }

    @Override public void close() throws SearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    public DfsSearchResult executeDfsPhase(InternalSearchRequest request) throws SearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        dfsPhase.execute(context);
        return context.dfsResult();
    }

    public QuerySearchResult executeQueryPhase(InternalSearchRequest request) throws SearchException {
        SearchContext context = createContext(request);
        activeContexts.put(context.id(), context);
        queryPhase.execute(context);
        return context.queryResult();
    }

    public QuerySearchResult executeQueryPhase(InternalScrollSearchRequest request) throws SearchException {
        SearchContext context = findContext(request.id());
        processScroll(request, context);
        queryPhase.execute(context);
        return context.queryResult();
    }

    public QuerySearchResult executeQueryPhase(QuerySearchRequest request) throws SearchException {
        SearchContext context = findContext(request.id());
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            throw new SearchException("Failed to set aggreagted df", e);
        }
        queryPhase.execute(context);
        return context.queryResult();
    }

    public QueryFetchSearchResult executeFetchPhase(InternalSearchRequest request) throws SearchException {
        SearchContext context = createContext(request);
        queryPhase.execute(context);
        shortcutDocIdsToLoad(context);
        fetchPhase.execute(context);
        if (context.scroll() != null) {
            activeContexts.put(context.id(), context);
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public QueryFetchSearchResult executeFetchPhase(QuerySearchRequest request) throws SearchException {
        SearchContext context = findContext(request.id());
        try {
            context.searcher().dfSource(new CachedDfSource(request.dfs(), context.similarityService().defaultSearchSimilarity()));
        } catch (IOException e) {
            throw new SearchException("Failed to set aggregated df", e);
        }
        queryPhase.execute(context);
        shortcutDocIdsToLoad(context);
        fetchPhase.execute(context);
        if (context.scroll() != null) {
            activeContexts.put(context.id(), context);
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public QueryFetchSearchResult executeFetchPhase(InternalScrollSearchRequest request) throws SearchException {
        SearchContext context = findContext(request.id());
        processScroll(request, context);
        queryPhase.execute(context);
        shortcutDocIdsToLoad(context);
        fetchPhase.execute(context);
        if (context.scroll() == null) {
            freeContext(request.id());
        }
        return new QueryFetchSearchResult(context.queryResult(), context.fetchResult());
    }

    public FetchSearchResult executeFetchPhase(FetchSearchRequest request) throws SearchException {
        SearchContext context = findContext(request.id());
        context.docIdsToLoad(request.docIds());
        fetchPhase.execute(context);
        if (context.scroll() == null) {
            freeContext(request.id());
        }
        return context.fetchResult();
    }

    private SearchContext findContext(long id) throws SearchContextMissingException {
        SearchContext context = activeContexts.get(id);
        if (context == null) {
            throw new SearchContextMissingException(id);
        }
        return context;
    }

    /**
     * 创建一个新的搜索上下文。
     * @param request 内部搜索请求
     * @return 返回创建的搜索上下文
     * @throws SearchException 如果搜索过程中出现异常
     */
    private SearchContext createContext(InternalSearchRequest request) throws SearchException {
        // 获取请求中指定的索引的服务
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        // 获取请求中指定的分片
        IndexShard indexShard = indexService.shardSafe(request.shardId());
        // 获取分片的搜索引擎搜索器
        Engine.Searcher engineSearcher = indexShard.searcher();

        // 创建一个表示搜索目标分片的SearchShardTarget对象
        SearchShardTarget shardTarget = new SearchShardTarget(clusterService.state().nodes().localNodeId(), request.index(), request.shardId());

        // 创建搜索上下文，包含递增的ID、搜索目标、请求的超时时间、查询提升因子、搜索源、类型、搜索引擎搜索器、索引服务
        SearchContext context = new SearchContext(idGenerator.incrementAndGet(), shardTarget, request.timeout(),
                request.queryBoost(), request.source(), request.types(), engineSearcher, indexService);

        // 初始化from和size参数
        context.from(request.from());
        context.size(request.size());

        // 初始化滚动参数
        context.scroll(request.scroll());

        // 解析搜索源
        parseSource(context);

        // 如果from和size仍未设置，则使用默认值
        if (context.from() == -1) {
            context.from(0);
        }
        if (context.size() == -1) {
            context.size(10);
        }

        // 返回创建的搜索上下文
        return context;
    }

    private void freeContext(long id) {
        SearchContext context = activeContexts.remove(id);
        if (context == null) {
            return;
        }
        freeContext(context);
    }

    private void freeContext(SearchContext context) {
        context.release();
    }

    /**
     * 解析搜索上下文的源数据。
     * @param context 搜索上下文
     * @throws SearchParseException 如果解析过程中出现异常
     */
    private void parseSource(SearchContext context) throws SearchParseException {
        try {
            // 创建JSON解析器，使用FastStringReader读取搜索上下文的源字符串
            JsonParser jp = jsonFactory.createJsonParser(new FastStringReader(context.source()));
            JsonToken token;

            // 循环遍历JSON对象中的所有字段
            while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    // 如果是字段名称，获取字段名
                    String fieldName = jp.getCurrentName();
                    jp.nextToken(); // 移动到字段的值

                    // 根据字段名获取对应的解析元素
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        // 如果没有找到对应的解析元素，抛出异常
                        throw new SearchParseException("No parser for element [" + fieldName + "]");
                    }
                    // 使用解析元素的parse方法来解析当前字段
                    element.parse(jp, context);
                } else if (token == null) {
                    // 如果已经到达了JSON的末尾，则退出循环
                    break;
                }
            }
        } catch (Exception e) {
            // 捕获解析过程中的任何异常，并抛出SearchParseException
            throw new SearchParseException("Failed to parse [" + context.source() + "]", e);
        }
    }

    private void shortcutDocIdsToLoad(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs();
        if (topDocs.scoreDocs.length < context.from()) {
            // no more docs...
            context.docIdsToLoad(new int[0]);
            return;
        }
        int totalSize = context.from() + context.size();
        int[] docIdsToLoad = new int[context.size()];
        int counter = 0;
        for (int i = context.from(); i < totalSize; i++) {
            if (i < topDocs.scoreDocs.length) {
                docIdsToLoad[counter] = topDocs.scoreDocs[i].doc;
            } else {
                break;
            }
            counter++;
        }
        if (counter < context.size()) {
            docIdsToLoad = Arrays.copyOfRange(docIdsToLoad, 0, counter);
        }
        context.docIdsToLoad(docIdsToLoad);
    }

    private void processScroll(InternalScrollSearchRequest request, SearchContext context) {
        // process scroll
        context.from(context.from() + context.size());
        context.scroll(request.scroll());
    }
}
