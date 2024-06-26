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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.server.search.search.dfs.CachedDfSource;
import org.server.search.util.lucene.docidset.DocIdSetCollector;

import java.io.IOException;
import java.util.List;

 
public class ContextIndexSearcher extends IndexSearcher {

    private SearchContext searchContext;

    private CachedDfSource dfSource;

    private boolean docIdSetEnabled;

    private List<Object>  docIdSet;

    public ContextIndexSearcher(SearchContext searchContext, IndexReader r) {
        super(r);
        this.searchContext = searchContext;
    }

    public void dfSource(CachedDfSource dfSource) {
        this.dfSource = dfSource;
    }

    public void enabledDocIdSet() {
        docIdSetEnabled = true;
    }

    public List<Object> docIdSet() {
        return docIdSet;
    }

}