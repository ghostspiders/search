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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatRangeDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import com.fasterxml.jackson.core.JsonToken;
import org.server.search.index.mapper.BoostFieldMapper;
import org.server.search.util.Numbers;

import java.io.IOException;

 
public class JsonBoostFieldMapper extends JsonNumberFieldMapper<Float> implements BoostFieldMapper {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final String NAME = "_boost";
        public static final Float NULL_VALUE = null;
        public static final FieldType INDEX = TextField.TYPE_STORED;
        public static final Field.Store STORE = Field.Store.NO;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonBoostFieldMapper> {

        protected Float nullValue = Defaults.NULL_VALUE;

        public Builder(String name) {
            super(name);
            builder = this;
            index = Defaults.INDEX;
            store = Defaults.STORE;
        }

        public Builder nullValue(float nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        @Override public JsonBoostFieldMapper build(BuilderContext context) {
            return new JsonBoostFieldMapper(name, buildIndexName(context),
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }


    private final Float nullValue;

    protected JsonBoostFieldMapper() {
        this(Defaults.NAME, Defaults.NAME);
    }

    protected JsonBoostFieldMapper(String name, String indexName) {
        this(name, indexName, Defaults.PRECISION_STEP, Defaults.INDEX, Defaults.STORE,
                Defaults.BOOST, Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Defaults.NULL_VALUE);
    }

    protected JsonBoostFieldMapper(String name, String indexName, int precisionStep, FieldType index, Field.Store store,
                                   float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                   Float nullValue) {
        super(name, indexName, name, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,null,null);
        this.nullValue = nullValue;
    }

    @Override protected int maxPrecisionStep() {
        return 32;
    }

    @Override public Float value(Field field) {
        byte[] value = field.binaryValue().bytes;
        if (value == null) {
            return Float.NaN;
        }
        return Numbers.bytesToFloat(value);
    }

    @Override public String indexedValue(String value) {
        return indexedValue(Float.parseFloat(value));
    }

    @Override public String indexedValue(Float value) {
        return String.valueOf(NumericUtils.floatToSortableInt(value));
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return FloatRangeDocValuesField.newSlowIntersectsQuery(indexName,
                lowerTerm == null ? null : new float[]{Float.parseFloat(lowerTerm)},
                upperTerm == null ? null : new float[]{Float.parseFloat(upperTerm)});
    }


    @Override public void parse(JsonParseContext jsonContext) throws IOException {
        // we override parse since we want to handle cases where it is not indexed and not stored (the default)
        float value = parsedFloatValue(jsonContext);
        if (!Float.isNaN(value)) {
//            jsonContext.doc().setBoost(value);
        }
        super.parse(jsonContext);
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        float value = parsedFloatValue(jsonContext);
        if (Float.isNaN(value)) {
            return null;
        }
//        jsonContext.doc().setBoost(value);
        Field field = null;
        if (stored()) {
            field = new Field(indexName, Numbers.floatToBytes(value), index);
            if (indexed()) {
                field.setTokenStream(popCachedStream(precisionStep).setFloatValue(value));
            }
        } else if (indexed()) {
            field = new Field(indexName, popCachedStream(precisionStep).setFloatValue(value),index);
        }
        return field;
    }

    private float parsedFloatValue(JsonParseContext jsonContext) throws IOException {
        float value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            if (nullValue == null) {
                return Float.NaN;
            }
            value = nullValue;
        } else {
            value = jsonContext.jp().getFloatValue();
        }
        return value;
    }

    @Override public SortField.Type sortType() {
        return SortField.Type.FLOAT;
    }
}
