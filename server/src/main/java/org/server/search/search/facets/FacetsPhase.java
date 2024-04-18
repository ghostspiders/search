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

package org.server.search.search.facets;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.search.DocIdSet;

import org.server.search.ElasticSearchException;
import org.server.search.ElasticSearchIllegalStateException;
import org.server.search.search.SearchParseElement;
import org.server.search.search.SearchPhase;
import org.server.search.search.internal.SearchContext;
import org.server.search.search.internal.SearchContextFacets;
import org.server.search.util.lucene.Lucene;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class FacetsPhase implements SearchPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("facets", new FacetsParseElement());
    }

    @Override public void execute(SearchContext context) throws ElasticSearchException {
        if (context.facets() == null) {
            return;
        }
        if (context.queryResult().facets() != null) {
            // no need to compute the facets twice, they should be computed on a per conext basis
            return;
        }

        SearchContextFacets contextFacets = context.facets();

        List<Facet> facets = Lists.newArrayListWithCapacity(2);
        if (contextFacets.queryFacets() != null) {
            for (SearchContextFacets.QueryFacet queryFacet : contextFacets.queryFacets()) {
                long count;
                if (contextFacets.queryType() == SearchContextFacets.QueryExecutionType.COLLECT) {
                    count = executeQueryCollectorCount(context, queryFacet);
                } else if (contextFacets.queryType() == SearchContextFacets.QueryExecutionType.IDSET) {
                    count = executeQueryIdSetCount(context, queryFacet);
                } else {
                    throw new ElasticSearchIllegalStateException("No matching for type [" + contextFacets.queryType() + "]");
                }
                facets.add(new CountFacet(queryFacet.name(), count));
            }
        }

        context.queryResult().facets(new Facets(facets));
    }

    private long executeQueryIdSetCount(SearchContext context, SearchContextFacets.QueryFacet queryFacet) {
        return Long.MIN_VALUE;
    }

    private long executeQueryCollectorCount(SearchContext context, SearchContextFacets.QueryFacet queryFacet) {
        Lucene.CountCollector countCollector = new Lucene.CountCollector(-1.0f);
        try {
            context.searcher().search(context.query(), countCollector);
        } catch (IOException e) {
            throw new FacetPhaseExecutionException(queryFacet.name(), "Failed to collect facets for query [" + queryFacet.query() + "]", e);
        }
        return countCollector.count();
    }
}
