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

import com.fasterxml.jackson.core.JsonToken;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.*;
import org.server.search.util.Numbers;

import java.io.IOException;

/**
 * 
 */
public class JsonLongFieldMapper extends JsonNumberFieldMapper<Long> {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final Long NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonLongFieldMapper> {

        protected Long nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(long nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonLongFieldMapper build(BuilderContext context) {
            return new JsonLongFieldMapper(name, buildIndexName(context), buildFullName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    private final Long nullValue;

    protected JsonLongFieldMapper(String name, String indexName, String fullName, int precisionStep, FieldType index, Field.Store store,
                                  float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                  Long nullValue) {
        super(name, indexName, fullName, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new StandardAnalyzer(), new StandardAnalyzer());
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 64;
    }

    @Override public Long value(Field field) {
        byte[] value = field.binaryValue().bytes;
        if (value == null) {
            return Long.MIN_VALUE;
        }
        return Numbers.bytesToLong(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Long.parseLong(value));
    }

    @Override public String indexedValue(Long value) {
        return String.valueOf(value);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return LongField.newRangeQuery(indexName,
                lowerTerm == null ? null : Long.parseLong(lowerTerm),
                upperTerm == null ? null : Long.parseLong(upperTerm));
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        long value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return null;
            }
            value = nullValue;
        } else {
            value = jsonContext.jp().getLongValue();
        }
        Field field = null;
        if (stored()) {
            field = new Field(indexName, Numbers.longToBytes(value), index);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setLongValue(value));
            }
        } else if (indexed()) {
            field = new Field(indexName, popCachedStream(precisionStep).setLongValue(value),index);
        }
        return field;
    }

    @Override public SortField.Type sortType() {
        return SortField.Type.LONG;
    }
}