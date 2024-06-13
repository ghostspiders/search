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

package org.server.search.search.controller;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.PriorityQueue;
import org.server.search.SearchIllegalStateException;
import org.server.search.lucene.ShardFieldDocSortedHitQueue;
import org.server.search.search.SearchHit;
import org.server.search.search.SearchShardTarget;
import org.server.search.search.dfs.AggregatedDfs;
import org.server.search.search.dfs.DfsSearchResult;
import org.server.search.search.facets.CountFacet;
import org.server.search.search.facets.Facet;
import org.server.search.search.facets.Facets;
import org.server.search.search.fetch.FetchSearchResult;
import org.server.search.search.fetch.FetchSearchResultProvider;
import org.server.search.search.internal.InternalSearchHit;
import org.server.search.search.internal.InternalSearchHits;
import org.server.search.search.internal.InternalSearchResponse;
import org.server.search.search.query.QuerySearchResult;
import org.server.search.search.query.QuerySearchResultProvider;
import org.server.search.util.trove.ExtTIntArrayList;
import org.server.search.util.trove.ExtTObjectIntHasMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

 
public class SearchPhaseController {
    /**
     * 一个空的ShardDoc数组，用作默认值或占位符。
     * ShardDoc可能代表一个分片文档或者与分片搜索结果相关的数据结构。
     */
    private static final ShardDoc[] EMPTY = new ShardDoc[0];
    /**
     * 聚合分布式频率搜索结果的方法。
     *
     * @param results 一个可迭代的DfsSearchResult对象集合，包含各个分片的DFS结果。
     * @return 返回一个AggregatedDfs对象，其中包含聚合后的词项频率和文档总数。
     */
    public AggregatedDfs aggregateDfs(Iterable<DfsSearchResult> results) {
        // 创建一个可扩展的哈希表，用于存储词项（Term）及其文档频率（df）。
        ExtTObjectIntHasMap<Term> dfMap = new ExtTObjectIntHasMap<Term>().defaultReturnValue(-1);
        // 用于累计所有分片的文档总数。
        int numDocs = 0;

        // 遍历所有的DfsSearchResult对象。
        for (DfsSearchResult result : results) {
            // 遍历分片结果中的每个词项及其频率。
            for (int i = 0; i < result.freqs().length; i++) {
                int freq = dfMap.get(result.terms()[i]);
                // 如果词项不在dfMap中，则使用当前频率作为初始值。
                if (freq == -1) {
                    freq = result.freqs()[i];
                } else {
                    // 如果词项已存在，则将当前频率累加到已存在的频率上。
                    freq += result.freqs()[i];
                }
                // 更新dfMap中的词项频率。
                dfMap.put(result.terms()[i], freq);
            }
            // 累加分片的文档总数。
            numDocs += result.numDocs();
        }

        // 创建并返回AggregatedDfs对象，包含聚合后的词项频率和文档总数。
        return new AggregatedDfs(dfMap, numDocs);
    }

    /**
     * 对查询搜索结果进行排序，生成一个ShardDoc数组。
     *
     * @param results 包含查询搜索结果提供者的集合。
     * @return 排序后的ShardDoc数组。
     */
    public ShardDoc[] sortDocs(Collection<? extends QuerySearchResultProvider> results) {
        // 如果结果集合为空，返回空的ShardDoc数组。
        if (Iterables.isEmpty(results)) {
            return EMPTY;
        }

        // 获取第一个结果提供者，用于初始化排序。
        QuerySearchResultProvider queryResultProvider = Iterables.get(results, 0);

        // 初始化文档总数计数器。
        int totalNumDocs = 0;

        // 计算队列大小，基于查询结果的from和size。
        int queueSize = queryResultProvider.queryResult().from() + queryResultProvider.queryResult().size();
        // 如果同时执行了查询和获取操作，调整队列大小以包含所有文档。
        if (queryResultProvider.includeFetch()) {
            queueSize *= results.size();
        }

        // 根据查询结果的topDocs类型，选择排序方式。
        PriorityQueue queue;
        if (queryResultProvider.queryResult().topDocs() instanceof TopFieldDocs) {
            // 使用字段排序，创建一个ShardFieldDocSortedHitQueue。
            queue = new ShardFieldDocSortedHitQueue(((TopFieldDocs) queryResultProvider.queryResult().topDocs()).fields, queueSize);
            // 遍历所有结果提供者，将文档添加到队列中。
            for (QuerySearchResultProvider resultProvider : results) {
                // ...（此处代码省略，与下文逻辑相同，只是使用ShardFieldDoc代替ShardScoreDoc）;
            }
        } else {
            // 使用分数排序，创建一个ScoreDocQueue。
            queue = new ScoreDocQueue(queueSize);
            // 遍历所有结果提供者，将文档添加到队列中。
            for (QuerySearchResultProvider resultProvider : results) {
                QuerySearchResult result = resultProvider.queryResult();
                ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
                totalNumDocs += scoreDocs.length;
                for (ScoreDoc doc : scoreDocs) {
                    ShardScoreDoc nodeScoreDoc = new ShardScoreDoc(result.shardTarget(), doc.doc, doc.score);
                    // 如果队列已满，中断循环。
                    if (queue.insertWithOverflow(nodeScoreDoc) == nodeScoreDoc) {
                        break;
                    }
                }
            }
        }

        // 计算结果数组的大小。
        int resultDocsSize = queryResultProvider.queryResult().size();
        // 如果同时执行了查询和获取操作，调整结果数组大小。
        if (queryResultProvider.includeFetch()) {
            resultDocsSize *= results.size();
        }
        // 如果总文档数小于队列大小，调整结果数组大小。
        if (totalNumDocs < queueSize) {
            resultDocsSize = totalNumDocs - queryResultProvider.queryResult().from();
        }

        // 如果结果数组大小小于等于0，返回空的ShardDoc数组。
        if (resultDocsSize <= 0) {
            return EMPTY;
        }

        // 创建ShardDoc数组，填充排序后的文档。
        ShardDoc[] shardDocs = new ShardDoc[resultDocsSize];
        for (int i = resultDocsSize - 1; i >= 0; i--) {
            // 从队列中弹出文档并放入数组。
            shardDocs[i] = (ShardDoc) queue.pop();
        }
        return shardDocs;
    }

    /**
     * 根据分片文档数组生成一个映射，包含每个分片目标的文档ID列表。
     *
     * @param shardDocs 分片文档数组，包含来自不同分片的文档信息。
     * @return 一个映射，其键是SearchShardTarget，值是ExtTIntArrayList，列表中包含每个分片目标的文档ID。
     */
    public Map<SearchShardTarget, ExtTIntArrayList> docIdsToLoad(ShardDoc[] shardDocs) {
        // 创建一个新的HashMap，用于存储结果。
        Map<SearchShardTarget, ExtTIntArrayList> result = Maps.newHashMap();

        // 遍历分片文档数组。
        for (ShardDoc shardDoc : shardDocs) {
            // 获取与当前分片目标相关联的文档ID列表。
            ExtTIntArrayList list = result.get(shardDoc.shardTarget());

            // 如果列表不存在，则创建一个新的ExtTIntArrayList并将其添加到映射中。
            if (list == null) {
                list = new ExtTIntArrayList();
                result.put(shardDoc.shardTarget(), list);
            }

            // 将当前分片文档的文档ID添加到列表中。
            list.add(shardDoc.docId());
        }

        // 返回包含文档ID列表的映射。
        return result;
    }
    /**
     * 合并排序后的文档、查询结果和获取结果，构建一个InternalSearchResponse对象。
     *
     * @param sortedDocs 排序后的ShardDoc数组，包含来自不同分片的文档信息。
     * @param queryResults 一个映射，其键是SearchShardTarget，值是查询搜索结果提供者。
     * @param fetchResults 一个映射，其键是SearchShardTarget，值是获取搜索结果提供者。
     * @return 一个InternalSearchResponse对象，包含合并后的搜索结果。
     */
    public InternalSearchResponse merge(ShardDoc[] sortedDocs, Map<SearchShardTarget, ? extends QuerySearchResultProvider> queryResults, Map<SearchShardTarget, ? extends FetchSearchResultProvider> fetchResults) {
        // merge facets
        Facets facets = null;
        if (!queryResults.isEmpty()) {
            // we rely on the fact that the order of facets is the same on all query results
            QuerySearchResult queryResult = queryResults.values().iterator().next().queryResult();

            if (queryResult.facets() != null && queryResult.facets().facets() != null && !queryResult.facets().facets().isEmpty()) {
                List<Facet> mergedFacets = Lists.newArrayListWithCapacity(2);
                for (Facet facet : queryResult.facets().facets()) {
                    if (facet.type() == Facet.Type.COUNT) {
                        mergedFacets.add(new CountFacet(facet.name(), 0));
                    } else {
                        throw new SearchIllegalStateException("Can't handle type [" + facet.type() + "]");
                    }
                }
                for (QuerySearchResultProvider queryResultProvider : queryResults.values()) {
                    List<Facet> queryFacets = queryResultProvider.queryResult().facets().facets();
                    for (int i = 0; i < mergedFacets.size(); i++) {
                        Facet queryFacet = queryFacets.get(i);
                        Facet mergedFacet = mergedFacets.get(i);
                        if (queryFacet.type() == Facet.Type.COUNT) {
                            ((CountFacet) mergedFacet).increment(((CountFacet) queryFacet).count());
                        }
                    }
                }
                facets = new Facets(mergedFacets);
            }
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        long totalHits = 0;
        for (QuerySearchResultProvider queryResultProvider : queryResults.values()) {
            totalHits += queryResultProvider.queryResult().topDocs().totalHits.value;
        }

        // clean the fetch counter
        for (FetchSearchResultProvider fetchSearchResultProvider : fetchResults.values()) {
            fetchSearchResultProvider.fetchResult().initCounter();
        }

        // merge hits
        List<SearchHit> hits = new ArrayList<SearchHit>();
        if (!fetchResults.isEmpty()) {
            for (ShardDoc shardDoc : sortedDocs) {
                FetchSearchResultProvider fetchResultProvider = fetchResults.get(shardDoc.shardTarget());
                if (fetchResultProvider == null) {
                    continue;
                }
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                int index = fetchResult.counterGetAndIncrement();
                SearchHit searchHit = fetchResult.hits().hits()[index];
                ((InternalSearchHit) searchHit).shard(fetchResult.shardTarget());
                hits.add(searchHit);
            }
        }
        InternalSearchHits searchHits = new InternalSearchHits(hits.toArray(new SearchHit[hits.size()]), totalHits);
        return new InternalSearchResponse(searchHits, facets);
    }
}
