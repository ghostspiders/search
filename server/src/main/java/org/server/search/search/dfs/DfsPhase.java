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

/**
 * @author kimchy (Shay Banon)
 */
public class DfsPhase implements SearchPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    public void execute(SearchContext context) {
        try {
            context.rewriteQuery();

            THashSet<Term> termsSet = new THashSet<Term>();
            context.query().extractTerms(termsSet);
            Term[] terms = termsSet.toArray(new Term[termsSet.size()]);
            int[] freqs = context.searcher().docFreqs(terms);

            context.dfsResult().termsAndFreqs(terms, freqs);
            context.dfsResult().numDocs(context.searcher().getIndexReader().numDocs());
        } catch (Exception e) {
            throw new DfsPhaseExecutionException(context);
        }
    }
}
