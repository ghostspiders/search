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

package org.server.search.index.mapper.json;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.SortField;
import org.server.search.util.gnu.trove.TIntObjectHashMap;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

 
public abstract class JsonNumberFieldMapper<T extends Number> extends JsonFieldMapper<T> {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final int PRECISION_STEP = 4;
        public static final FieldType INDEX = new FieldType();
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public abstract static class Builder<T extends Builder, Y extends JsonNumberFieldMapper> extends JsonFieldMapper.Builder<T, Y> {

        protected int precisionStep = Defaults.PRECISION_STEP;

        public Builder(String name) {
            super(name);
            this.index = Defaults.INDEX;
            this.omitNorms = Defaults.OMIT_NORMS;
            this.omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        public T precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return builder;
        }
    }

    private static final ThreadLocal<TIntObjectHashMap<Deque<CachedNumericTokenStream>>> cachedStreams = new ThreadLocal<TIntObjectHashMap<Deque<CachedNumericTokenStream>>>() {
        @Override protected TIntObjectHashMap<Deque<CachedNumericTokenStream>> initialValue() {
            return new TIntObjectHashMap<Deque<CachedNumericTokenStream>>();
        }
    };

    protected final int precisionStep;

    protected JsonNumberFieldMapper(String name, String indexName, String fullName, int precisionStep,
                                    FieldType index, Field.Store store,
                                    float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                    Analyzer indexAnalyzer, Analyzer searchAnalyzer) {
        super(name, indexName, fullName, index, store, Defaults.INDEX, boost, omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
            this.precisionStep = Integer.MAX_VALUE;
        } else {
            this.precisionStep = precisionStep;
        }
    }

    protected abstract int maxPrecisionStep();

    public int precisionStep() {
        return this.precisionStep;
    }

    /**
     * Override the defualt behavior (to return the string, and reutrn the actual Number instance).
     */
    @Override public Object valueForSearch(Field field) {
        return value(field);
    }

    @Override public String valueAsString(Field field) {
        return value(field).toString();
    }

    @Override public abstract SortField.Type sortType();

    /**
     * Removes a cached numeric token stream. The stream will be returned to the cahed once it is used
     * sicne it implements the end method.
     */
    protected CachedNumericTokenStream popCachedStream(int precisionStep) {
        Deque<CachedNumericTokenStream> deque = cachedStreams.get().get(precisionStep);
        if (deque == null) {
            deque = new ArrayDeque<CachedNumericTokenStream>();
            cachedStreams.get().put(precisionStep, deque);
            deque.add(new CachedNumericTokenStream(precisionStep));
        }
        if (deque.isEmpty()) {
            deque.add(new CachedNumericTokenStream(precisionStep));
        }
        return deque.pollFirst();
    }

    /**
     * A wrapper around a numeric stream allowing to reuse it by implementing the end method which returns
     * this stream back to the thread local cache.
     */
    protected static final class CachedNumericTokenStream extends TokenStream {

        private final int precisionStep;


        public CachedNumericTokenStream(int precisionStep) {
            super();
            this.precisionStep = precisionStep;
        }

        public void end() throws IOException {

        }

        /**
         * Close the input TokenStream.
         */
        public void close() throws IOException {
            cachedStreams.get().get(precisionStep).add(this);
        }

        /**
         * Reset the filter as well as the input TokenStream.
         */
        public void reset() throws IOException {
        }

        @Override public boolean incrementToken() throws IOException {
            return true;
        }

        public CachedNumericTokenStream setIntValue(int value) {
            return this;
        }

        public CachedNumericTokenStream setLongValue(long value) {
            return this;
        }

        public CachedNumericTokenStream setFloatValue(float value) {
            return this;
        }

        public CachedNumericTokenStream setDoubleValue(double value) {
            return this;
        }
    }
}
