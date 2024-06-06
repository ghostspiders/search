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

package org.server.search.util.lucene.docidset;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import java.util.BitSet;

 
public class DocIdSetCollector implements Collector {

    private final Collector collector;

    private final BitSet docIdSet;

    private int base;

    public DocIdSetCollector(Collector collector, IndexReader reader) {
        this.collector = collector;
        this.docIdSet = new BitSet(reader.maxDoc());
    }

    public BitSet  docIdSet() {
        return docIdSet;
    }


    /**
     * Create a new {@link LeafCollector collector} to collect the given context.
     *
     * @param context next atomic reader context
     */
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context){
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer){
            }

            @Override
            public void collect(int doc)  {
                docIdSet.nextSetBit(base + doc);
            }
        };
    }

    /**
     * Indicates what features are required from the scorer.
     */
    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }
}
