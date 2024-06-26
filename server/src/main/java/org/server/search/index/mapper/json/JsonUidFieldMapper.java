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
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.server.search.index.mapper.MapperParsingException;
import org.server.search.index.mapper.Uid;
import org.server.search.index.mapper.UidFieldMapper;
import org.server.search.util.lucene.Lucene;

import java.io.IOException;

 
public class JsonUidFieldMapper extends JsonFieldMapper<Uid> implements UidFieldMapper {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final String NAME = "_uid";
        public static final FieldType INDEX = TextField.TYPE_STORED;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
    }

    public static class Builder extends JsonMapper.Builder<Builder, JsonUidFieldMapper> {

        protected String indexName;

        public Builder(String name) {
            super(name);
            this.indexName = name;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        @Override public JsonUidFieldMapper build(BuilderContext context) {
            return new JsonUidFieldMapper(name, indexName);
        }
    }

    protected JsonUidFieldMapper() {
        this(Defaults.NAME);
    }

    protected JsonUidFieldMapper(String name) {
        this(name, name);
    }

    protected JsonUidFieldMapper(String name, String indexName) {
        super(name, indexName, name, Defaults.INDEX, Field.Store.YES, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
    }

    @Override public String name() {
        return this.name;
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        if (jsonContext.id() == null) {
            throw new MapperParsingException("No id found while parsing the json source");
        }
        jsonContext.uid(Uid.createUid(jsonContext.stringBuilder(), jsonContext.type(), jsonContext.id()));
        return new KeywordField(name, jsonContext.uid(),store);
    }

    @Override public Uid value(Field field) {
        return Uid.createUid(field.stringValue());
    }

    @Override public String valueAsString(Field field) {
        return field.stringValue();
    }

    @Override public String indexedValue(String value) {
        return value;
    }

    @Override public Term term(String type, String id) {
        return term(Uid.createUid(type, id));
    }

    @Override public Term term(String uid) {
        return new Term(indexName, uid);
    }
}
