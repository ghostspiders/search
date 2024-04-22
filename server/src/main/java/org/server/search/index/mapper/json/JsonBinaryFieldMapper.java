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
import com.fasterxml.jackson.core.JsonToken;
import org.apache.lucene.document.FieldType;

import java.io.IOException;

/**
 * 
 */
public class JsonBinaryFieldMapper extends JsonFieldMapper<byte[]> {

    public static class Builder extends JsonFieldMapper.Builder<Builder, JsonBinaryFieldMapper> {

        public Builder(String name) {
            super(name);
            builder = this;
        }

        @Override public JsonBinaryFieldMapper build(BuilderContext context) {
            return new JsonBinaryFieldMapper(name, buildIndexName(context), buildFullName(context));
        }
    }

    protected JsonBinaryFieldMapper(String name, String indexName, String fullName) {
        super(name, indexName, fullName, new FieldType(), Field.Store.YES, new FieldType(), 1.0f, true, true, null, null);
    }


    @Override
    public byte[] value(Field field) {

        return new byte[0];
    }

    @Override
    public String valueAsString(Field field) {
        return null;
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        byte[] value;
        if (jsonContext.jp().getCurrentToken() == JsonToken.VALUE_NULL) {
            return null;
        } else {
            value = jsonContext.jp().getBinaryValue();
        }
        if (value == null) {
            return null;
        }

        return new Field(indexName, value, new FieldType());
    }
}