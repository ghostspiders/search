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
import org.apache.lucene.document.LongRangeDocValuesField;
import org.apache.lucene.search.*;
import org.apache.lucene.util.NumericUtils;
import org.server.search.util.Numbers;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonDateFieldMapper extends JsonNumberFieldMapper<Long> {

    public static class Defaults extends JsonNumberFieldMapper.Defaults {
        public static final DateTimeFormatter DATE_TIME_FORMATTER =
                ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC);

        public static final String NULL_VALUE = null;
    }

    public static class Builder extends JsonNumberFieldMapper.Builder<Builder, JsonDateFieldMapper> {

        protected String nullValue = Defaults.NULL_VALUE;

        protected DateTimeFormatter dateTimeFormatter = Defaults.DATE_TIME_FORMATTER;

        public Builder(String name) {
            super(name);
            builder = this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Builder dateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        @Override public JsonDateFieldMapper build(BuilderContext context) {
            return new JsonDateFieldMapper(name, buildIndexName(context), buildFullName(context), dateTimeFormatter,
                    precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions, nullValue);
        }
    }


    private final DateTimeFormatter dateTimeFormatter;

    private final String nullValue;

    protected JsonDateFieldMapper(String name, String indexName, String fullName, DateTimeFormatter dateTimeFormatter, int precisionStep,
                                  FieldType index, Field.Store store,
                                  float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                  String nullValue) {
        super(name, indexName, fullName, precisionStep, index, store, boost, omitNorms, omitTermFreqAndPositions,
                new StandardAnalyzer(), new StandardAnalyzer());
        this.dateTimeFormatter = dateTimeFormatter;
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

    @Override public String valueAsString(Field field) {
        return dateTimeFormatter.print(value(field));
    }


    @Override public String indexedValue(String value) {
        return String.valueOf(dateTimeFormatter.parseMillis(value));
    }

    @Override public String indexedValue(Long value) {
        return String.valueOf(value);
    }

    @Override public Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
        return LongRangeDocValuesField.newSlowIntersectsQuery(indexName,
                lowerTerm == null ? null : new long[]{dateTimeFormatter.parseMillis(lowerTerm)},
                upperTerm == null ? null : new long[]{dateTimeFormatter.parseMillis(upperTerm)});
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        String dateAsString;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            dateAsString = nullValue;
        } else {
            dateAsString = jsonContext.jp().getText();
        }
        if (dateAsString == null) {
            return null;
        }
        long value = dateTimeFormatter.parseMillis(dateAsString);
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