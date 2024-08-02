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

package org.server.search.search.dfs;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.Term;
import org.server.search.search.SearchParseElement;
import org.server.search.search.SearchPhase;
import org.server.search.search.internal.SearchContext;
import org.server.search.util.gnu.trove.THashSet;

import java.util.Map;

 
public class DfsPhase implements SearchPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    public void execute(SearchContext context) {
        // 尝试执行以下代码块，如果发生异常，则捕获并处理
        try {
            // 重写查询，可能涉及查询的优化或转换
            context.rewriteQuery();
            // 创建一个线程安全的集合来存储查询中提取的词项
            THashSet<Term> termsSet = new THashSet<Term>();
            // 从查询中提取所有词项，并将它们添加到termsSet中
            context.query().extractTerms(termsSet);
            // 将termsSet转换为Term数组
            Term[] terms = termsSet.toArray(new Term[termsSet.size()]);
            // 使用SearchContext中的搜索器获取每个词项在整个索引中的文档频率
            int[] freqs = new int[terms.length];
            // 遍历词条数组，获取每个词条的文档频率
            for (int i = 0; i < terms.length; i++) {
                freqs[i] =  context.searcher().getIndexReader().docFreq(terms[i]);
            }
            // 将收集到的词项及其频率信息存储到分布式搜索结果中
            context.dfsResult().termsAndFreqs(terms, freqs);
            // 获取索引中的文档总数，并将其设置到分布式搜索结果中
            context.dfsResult().numDocs(context.searcher().getIndexReader().numDocs());
        } catch (Exception e) {
            // 如果执行过程中发生异常，抛出DfsPhaseExecutionException异常
            throw new DfsPhaseExecutionException(context);
        }
    }
}
