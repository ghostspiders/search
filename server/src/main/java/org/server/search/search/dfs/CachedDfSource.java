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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.server.search.SearchException;
import org.server.search.index.engine.Engine;

import java.io.IOException;

/**
 * 
 */
public class CachedDfSource implements Engine.Searcher {

    private final AggregatedDfs dfs;

    public CachedDfSource(AggregatedDfs dfs, Similarity similarity) throws IOException {
        this.dfs = dfs;
    }

    public int docFreq(Term term) {
        int df = dfs.dfMap().get(term);
        if (df == -1) {
            throw new IllegalArgumentException("df for term " + term.text() + " not available");
        }
        return df;
    }

    public int[] docFreqs(Term[] terms) {
        int[] result = new int[terms.length];
        for (int i = 0; i < terms.length; i++) {
            result[i] = docFreq(terms[i]);
        }
        return result;
    }

    public int maxDoc() {
        return dfs.numDocs();
    }

    public Query rewrite(Query query) {
        // this is a bit of a hack. We know that a query which
        // creates a Weight based on this Dummy-Searcher is
        // always already rewritten (see preparedWeight()).
        // Therefore we just return the unmodified query here
        return query;
    }

    public void close() {
        throw new UnsupportedOperationException();
    }

    public Document doc(int i) {
        throw new UnsupportedOperationException();
    }

    public Explanation explain(Weight weight, int doc) {
        throw new UnsupportedOperationException();
    }


    @Override
    public IndexReader reader() {
        return null;
    }

    @Override
    public IndexSearcher searcher() {
        return null;
    }

    @Override
    public boolean release() throws SearchException {
        return false;
    }
}
