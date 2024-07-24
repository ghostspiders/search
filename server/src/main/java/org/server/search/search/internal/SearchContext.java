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

package org.server.search.search.internal;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.server.search.SearchException;
import org.server.search.index.IndexService;
import org.server.search.index.engine.Engine;
import org.server.search.index.mapper.MapperService;
import org.server.search.index.query.IndexQueryParser;
import org.server.search.index.query.IndexQueryParserMissingException;
import org.server.search.index.query.IndexQueryParserService;
import org.server.search.index.similarity.SimilarityService;
import org.server.search.search.Scroll;
import org.server.search.search.SearchShardTarget;
import org.server.search.search.dfs.DfsSearchResult;
import org.server.search.search.fetch.FetchSearchResult;
import org.server.search.search.query.QuerySearchResult;
import org.server.search.util.TimeValue;
import org.server.search.util.lease.Releasable;

import java.io.IOException;

 
public class SearchContext implements Releasable {

    // 唯一标识符
    private final long id;

    // 搜索请求的原始JSON源字符串
    private final String source;

    // 搜索引擎搜索器，用于执行搜索操作
    private final Engine.Searcher engineSearcher;

    // 索引服务，提供与索引相关的操作和管理
    private final IndexService indexService;

    // 上下文索引搜索器，封装了对索引的搜索能力
    private final ContextIndexSearcher searcher;

    // 分布式频率筛选（DFS）搜索结果，用于收集文档频率信息
    private final DfsSearchResult dfsResult;

    // 查询搜索结果，包含查询阶段的结果数据
    private final QuerySearchResult queryResult;

    // 获取搜索结果，包含获取阶段的结果数据
    private final FetchSearchResult fetchResult;

    // 搜索操作的超时时间
    private final TimeValue timeout;

    // 查询提升因子，用于影响搜索结果的相关性得分
    private final float queryBoost;

    // 滚动搜索对象，用于实现搜索结果的滚动加载
    private Scroll scroll;

    // 是否对搜索结果进行解释，提供额外的执行信息
    private boolean explain;

    // 要检索的特定字段名称数组
    private String[] fieldNames;

    // 搜索结果的起始位置，默认为-1，表示无特定起始位置
    private int from = -1;

    // 搜索结果的数量，默认为-1，表示使用默认大小
    private int size = -1;

    // 要搜索的索引类型数组
    private String[] types;

    // 搜索结果的排序参数
    private Sort sort;

    // 查询解析器名称，用于指定使用哪个解析器来解析查询
    private String queryParserName;

    // 搜索查询对象，包含实际的查询逻辑
    private Query query;

    // 要加载的文档ID数组
    private int[] docIdsToLoad;

    // 搜索上下文的聚合面值，用于收集和计算聚合数据
    private SearchContextFacets facets;

    // 标记查询是否已经被重写，例如通过查询重写器优化查询
    private boolean queryRewritten;

    public SearchContext(long id, SearchShardTarget shardTarget, TimeValue timeout, float queryBoost, String source,
                         String[] types, Engine.Searcher engineSearcher, IndexService indexService) {
        this.id = id;
        this.timeout = timeout;
        this.queryBoost = queryBoost;
        this.source = source;
        this.types = types;
        this.engineSearcher = engineSearcher;
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexService = indexService;

        this.searcher = new ContextIndexSearcher(this, engineSearcher.reader());
    }

    @Override public boolean release() throws SearchException {
        engineSearcher.release();
        return true;
    }

    public long id() {
        return this.id;
    }

    public String source() {
        return source;
    }

    public String[] types() {
        return types;
    }

    public float queryBoost() {
        return queryBoost;
    }

    public Scroll scroll() {
        return this.scroll;
    }

    public SearchContext scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public SearchContextFacets facets() {
        return facets;
    }

    public SearchContext facets(SearchContextFacets facets) {
        this.facets = facets;
        return this;
    }

    public Engine.Searcher engineSearcher() {
        return this.engineSearcher;
    }

    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    public IndexQueryParser queryParser() throws IndexQueryParserMissingException {
        if (queryParserName != null) {
            IndexQueryParser queryParser = queryParserService().indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
            return queryParser;
        }
        return queryParserService().defaultIndexQueryParser();
    }

    public MapperService mapperService() {
        return indexService.mapperService();
    }

    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
    }

    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    public TimeValue timeout() {
        return timeout;
    }

    public SearchContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    public Sort sort() {
        return this.sort;
    }

    public String queryParserName() {
        return queryParserName;
    }

    public SearchContext queryParserName(String queryParserName) {
        this.queryParserName = queryParserName;
        return this;
    }

    public SearchContext query(Query query) {
        if (query == null) {
            this.query = query;
            return this;
        }
        queryRewritten = false;
        this.query = query;
        return this;
    }

    public Query query() {
        return this.query;
    }

    public int from() {
        return from;
    }

    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    public String[] fieldNames() {
        return fieldNames;
    }

    public SearchContext fieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public boolean explain() {
        return explain;
    }

    public void explain(boolean explain) {
        this.explain = explain;
    }

    public SearchContext rewriteQuery() throws IOException {
        if (queryRewritten) {
            return this;
        }
        query = query.rewrite(searcher.getIndexReader());
        queryRewritten = true;
        return this;
    }

    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    public SearchContext docIdsToLoad(int[] docIdsToLoad) {
        this.docIdsToLoad = docIdsToLoad;
        return this;
    }

    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public FetchSearchResult fetchResult() {
        return fetchResult;
    }
}
