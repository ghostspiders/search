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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.server.search.index.mapper.FieldMapper;
import org.server.search.index.mapper.FieldMapperListener;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class JsonFieldMapper<T> implements FieldMapper<T>, JsonMapper {

    public static class Defaults {
        FieldType customType = new FieldType();
        public static final FieldType INDEX = new FieldType();
        public static final Field.Store STORE = Field.Store.NO;
        public static final FieldType  TERM_VECTOR = new FieldType();
        public static final float BOOST = 1.0f;
        public static final boolean OMIT_NORMS = false;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = false;
    }

    public abstract static class Builder<T extends Builder, Y extends JsonFieldMapper> extends JsonMapper.Builder<T, Y> {

        protected FieldType index = Defaults.INDEX;

        protected Field.Store store = Defaults.STORE;

        protected FieldType termVector = Defaults.TERM_VECTOR;

        protected float boost = Defaults.BOOST;

        protected boolean omitNorms = Defaults.OMIT_NORMS;

        protected boolean omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;

        protected String indexName;

        protected Analyzer indexAnalyzer;

        protected Analyzer searchAnalyzer;

        public Builder(String name) {
            super(name);
            indexName = name;
        }

        public T index(FieldType index) {
            this.index = index;
            return builder;
        }

        public T store(Field.Store store) {
            this.store = store;
            return builder;
        }

        public T termVector(FieldType termVector) {
            this.termVector = termVector;
            return builder;
        }

        public T boost(float boost) {
            this.boost = boost;
            return builder;
        }

        public T omitNorms(boolean omitNorms) {
            this.omitNorms = omitNorms;
            return builder;
        }

        public T omitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
            this.omitTermFreqAndPositions = omitTermFreqAndPositions;
            return builder;
        }

        public T indexName(String indexName) {
            this.indexName = indexName;
            return builder;
        }

        public T indexAnalyzer(Analyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            if (this.searchAnalyzer == null) {
                this.searchAnalyzer = indexAnalyzer;
            }
            return builder;
        }

        public T searchAnalyzer(Analyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return builder;
        }

        protected String buildIndexName(BuilderContext context) {
            String actualIndexName = indexName == null ? name : indexName;
            return context.path().pathAsText(actualIndexName);
        }

        protected String buildFullName(BuilderContext context) {
            return context.path().fullPathAsText(name);
        }
    }

    protected final String name;

    protected final String indexName;

    protected final String fullName;

    protected final FieldType index;

    protected final Field.Store store;

    protected final FieldType termVector;

    protected final float boost;

    protected final boolean omitNorms;

    protected final boolean omitTermFreqAndPositions;

    protected final Analyzer indexAnalyzer;

    protected final Analyzer searchAnalyzer;

    protected JsonFieldMapper(String name, String indexName, String fullName, FieldType index, Field.Store store, FieldType termVector,
                              float boost, boolean omitNorms, boolean omitTermFreqAndPositions, Analyzer indexAnalyzer, Analyzer searchAnalyzer) {
        this.name = name;
        this.indexName = indexName;
        this.fullName = fullName;
        this.index = index;
        this.store = store;
        this.termVector = termVector;
        this.boost = boost;
        this.omitNorms = omitNorms;
        this.omitTermFreqAndPositions = omitTermFreqAndPositions;
        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String indexName() {
        return this.indexName;
    }

    @Override public String fullName() {
        return this.fullName;
    }

    @Override public FieldType index() {
        return this.index;
    }

    @Override public Field.Store store() {
        return this.store;
    }

    @Override public boolean stored() {
        return store == Field.Store.YES;
    }

    @Override public boolean indexed() {
        return index.stored();
    }

    @Override public boolean analyzed() {
        return index.tokenized();
    }

    @Override public FieldType termVector() {
        return this.termVector;
    }

    @Override public float boost() {
        return this.boost;
    }

    @Override public boolean omitNorms() {
        return this.omitNorms;
    }

    @Override public boolean omitTermFreqAndPositions() {
        return this.omitTermFreqAndPositions;
    }

    @Override public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override public void parse(JsonParseContext jsonContext) throws IOException {
        if (!indexed() && !stored()) {
            return;
        }
        Field field = parseCreateField(jsonContext);
        if (field == null) {
            return;
        }
//        field.setOmitNorms(omitNorms);
//        field.setOmitTermFreqAndPositions(omitTermFreqAndPositions);
//        field.setBoost(boost);
        jsonContext.doc().add(field);
    }

    protected abstract Field parseCreateField(JsonParseContext jsonContext) throws IOException;

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        fieldMapperListener.fieldMapper(this);
    }

    @Override public Object valueForSearch(Field field) {
        return valueAsString(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public String indexedValue(T value) {
        return value.toString();
    }

    @Override public Query fieldQuery(String value) {
        return new TermQuery(new Term(indexName, indexedValue(value)));
    }

    @Override public Query fieldFilter(String value) {
        return new TermQuery(new Term(indexName, indexedValue(value)));
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return new TermRangeQuery(indexName,
                lowerTerm == null ? null : new BytesRef(indexedValue(lowerTerm)),
                upperTerm == null ? null : new BytesRef(indexedValue(upperTerm)),
                includeLower, includeUpper);
    }

    @Override public SortField.Type sortType() {
        return SortField.Type.STRING;
    }
}
