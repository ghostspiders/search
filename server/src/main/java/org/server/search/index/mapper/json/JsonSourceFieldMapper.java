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

import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StoredFieldVisitor;
import org.server.search.index.mapper.MapperCompressionException;
import org.server.search.index.mapper.SourceFieldMapper;
import org.server.search.util.io.compression.Compressor;
import org.server.search.util.io.compression.ZipCompressor;
import org.server.search.util.lucene.Lucene;

import java.io.IOException;

 
public class JsonSourceFieldMapper extends JsonFieldMapper<String> implements SourceFieldMapper {

    public static class Defaults extends JsonFieldMapper.Defaults {
        public static final String NAME = "_source";
        public static final boolean ENABLED = true;
        public static final FieldType INDEX = TextField.TYPE_STORED;
        public static final Field.Store STORE = Field.Store.YES;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
        public static final Compressor COMPRESSOR = new ZipCompressor();
        public static final int NO_COMPRESSION = -1;
    }

    public static class Builder extends JsonMapper.Builder<Builder, JsonSourceFieldMapper> {

        private boolean enabled = Defaults.ENABLED;

        private Compressor compressor = Defaults.COMPRESSOR;

        private int compressionThreshold = Defaults.NO_COMPRESSION;

        public Builder(String name) {
            super(name);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder compressor(Compressor compressor) {
            this.compressor = compressor;
            return this;
        }

        public Builder compressionThreshold(int compressionThreshold) {
            this.compressionThreshold = compressionThreshold;
            return this;
        }

        @Override public JsonSourceFieldMapper build(BuilderContext context) {
            return new JsonSourceFieldMapper(name, enabled, compressionThreshold, compressor);
        }
    }

    private final boolean enabled;

    private final Compressor compressor;

    // the size of the source file that we will perform compression for
    private final int compressionThreshold;

    private final StoredFieldVisitor  fieldSelector;

    protected JsonSourceFieldMapper() {
        this(Defaults.NAME, Defaults.ENABLED);
    }

    protected JsonSourceFieldMapper(String name, boolean enabled) {
        this(name, enabled, Defaults.NO_COMPRESSION, Defaults.COMPRESSOR);
    }

    protected JsonSourceFieldMapper(String name, boolean enabled, int compressionThreshold, Compressor compressor) {
        super(name, name, name, Defaults.INDEX, Defaults.STORE, Defaults.TERM_VECTOR, Defaults.BOOST,
                Defaults.OMIT_NORMS, Defaults.OMIT_TERM_FREQ_AND_POSITIONS, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER);
        this.enabled = enabled;
        this.compressionThreshold = compressionThreshold;
        this.compressor = compressor;
        this.fieldSelector = new DocumentStoredFieldVisitor(indexName);
    }

    public boolean enabled() {
        return this.enabled;
    }

    public StoredFieldVisitor fieldSelector() {
        return this.fieldSelector;
    }

    @Override protected Field parseCreateField(JsonParseContext jsonContext) throws IOException {
        if (!enabled) {
            return null;
        }
        Field sourceField;
        if (compressionThreshold == Defaults.NO_COMPRESSION || jsonContext.source().length() < compressionThreshold) {
            sourceField = new Field(name, jsonContext.source(), index);
        } else {
            try {
                sourceField = new Field(name, compressor.compressString(jsonContext.source()), index);
            } catch (IOException e) {
                throw new MapperCompressionException("Failed to compress data", e);
            }
        }
        return sourceField;
    }

    @Override public String value(Document document) {
        IndexableField field = document.getField(indexName);
        return field == null ? null : field.stringValue();
    }

    @Override public String value(Field field) {
        if (field.stringValue() != null) {
            return field.stringValue();
        }
        byte[] compressed = field.binaryValue().bytes;
        if (compressed == null) {
            return null;
        }
        try {
            return compressor.decompressString(compressed);
        } catch (IOException e) {
            throw new MapperCompressionException("Failed to decompress data", e);
        }
    }

    @Override public String valueAsString(Field field) {
        return value(field);
    }

    @Override public String indexedValue(String value) {
        return value;
    }

}
