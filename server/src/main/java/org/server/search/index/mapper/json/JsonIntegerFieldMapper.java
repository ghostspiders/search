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
import org.apache.lucene.document.IntField;
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import org.server.search.util.Numbers;
import org.server.search.util.Strings;

import java.io.IOException;

 
public class JsonIntegerFieldMapper extends JsonNumberFieldMapper<Integer> {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final Integer NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonIntegerFieldMapper> {

        protected Integer nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(int nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonIntegerFieldMapper build(BuilderContext context) {
            return new JsonIntegerFieldMapper(name, buildIndexName(context), buildFullName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }

    private final Integer nullValue;

    protected JsonIntegerFieldMapper(String name, String indexName, String fullName, int precisionStep, FieldType index, Field.Store store,
                                     float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                     Integer nullValue) {
        super(name, indexName, fullName, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new StandardAnalyzer(), new StandardAnalyzer());
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 32;
    }

    @Override public Integer value(Field field) {
        byte[] value = field.binaryValue().bytes;
        if (value == null) {
            return Integer.MIN_VALUE;
        }
        return Numbers.bytesToInt(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Integer.parseInt(value));
    }

    @Override public String indexedValue(Integer value) {
        return String.valueOf(value);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return IntField.newRangeQuery(indexName,
                lowerTerm == null ? null : Integer.parseInt(lowerTerm),
                upperTerm == null ? null : Integer.parseInt(upperTerm));
    }


    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        int value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return null;
            }
            value = nullValue;
        } else {
            value = jsonContext.jp().getIntValue();
        }
        Field field = null;
        if (stored()) {
            field = new Field(indexName, Numbers.intToBytes(value), index);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setIntValue(value));
            }
        } else if (indexed()) {
            field = new Field(indexName, popCachedStream(precisionStep).setIntValue(value),index);
        }
        return field;
    }

    @Override public SortField.Type sortType() {
        return SortField.Type.INT;
    }
}
