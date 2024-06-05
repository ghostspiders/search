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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.server.search.index.mapper.*;
import org.server.search.util.Nullable;
import org.server.search.util.Preconditions;
import org.server.search.util.io.FastStringReader;
import org.server.search.util.json.Jackson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.*;

 
public class JsonDocumentMapper implements DocumentMapper {

    public static class Builder {

        // JsonUidFieldMapper，用于处理文档的唯一标识符（UID）的映射
        private JsonUidFieldMapper uidFieldMapper = new JsonUidFieldMapper();

        // JsonIdFieldMapper，用于处理文档ID的映射
        private JsonIdFieldMapper idFieldMapper = new JsonIdFieldMapper();

        // JsonTypeFieldMapper，用于处理文档类型的映射
        private JsonTypeFieldMapper typeFieldMapper = new JsonTypeFieldMapper();

        // JsonSourceFieldMapper，用于处理文档原始JSON源数据的映射
        private JsonSourceFieldMapper sourceFieldMapper = new JsonSourceFieldMapper();

        // JsonBoostFieldMapper，用于处理文档提升因子（boost）的映射
        private JsonBoostFieldMapper boostFieldMapper = new JsonBoostFieldMapper();

        // indexAnalyzer，用于索引时的文本分析，可能需要设置特定的分析器
        private Analyzer indexAnalyzer;

        // searchAnalyzer，用于搜索时的文本分析，可能需要设置特定的分析器
        private Analyzer searchAnalyzer;

        // rootObjectMapper，根对象的JSON映射器，用于处理整个JSON文档的映射
        private final JsonObjectMapper rootObjectMapper;

        // mappingSource，存储映射定义的JSON源数据
        private String mappingSource;

        // builderContext，构建器上下文，用于在构建映射时存储和传递上下文信息
        // JsonPath(1)表示这是根路径
        private JsonMapper.BuilderContext builderContext = new JsonMapper.BuilderContext(new JsonPath(1));

        public Builder(JsonObjectMapper.Builder builder) {
            this.rootObjectMapper = builder.build(builderContext);
        }

        public Builder sourceField(JsonSourceFieldMapper.Builder builder) {
            this.sourceFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder idField(JsonIdFieldMapper.Builder builder) {
            this.idFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder uidField(JsonUidFieldMapper.Builder builder) {
            this.uidFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder typeField(JsonTypeFieldMapper.Builder builder) {
            this.typeFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder boostField(JsonBoostFieldMapper.Builder builder) {
            this.boostFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder mappingSource(String mappingSource) {
            this.mappingSource = mappingSource;
            return this;
        }

        public Builder indexAnalyzer(Analyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public boolean hasIndexAnalyzer() {
            return indexAnalyzer != null;
        }

        public Builder searchAnalyzer(Analyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public boolean hasSearchAnalyzer() {
            return searchAnalyzer != null;
        }

        public JsonDocumentMapper build() {
            Preconditions.checkNotNull(rootObjectMapper, "Json mapper builder must have the root object mapper set");
            return new JsonDocumentMapper(rootObjectMapper, uidFieldMapper, idFieldMapper, typeFieldMapper,
                    sourceFieldMapper, indexAnalyzer, searchAnalyzer, boostFieldMapper, mappingSource);
        }
    }


    private ThreadLocal<JsonParseContext> cache = new ThreadLocal<JsonParseContext>() {
        @Override protected JsonParseContext initialValue() {
            return new JsonParseContext(JsonDocumentMapper.this, new JsonPath(0));
        }
    };

    // JsonFactory用于创建JSON解析器和生成器，这里使用的是Jackson库的默认工厂
    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    // 文档类型，用于标识文档的类型
    private final String type;

    // 映射源代码，存储了文档的映射定义
    private final String mappingSource;

    // 用于处理文档唯一标识符（UID）的JSON字段映射器
    private final JsonUidFieldMapper uidFieldMapper;

    // 用于处理文档ID的JSON字段映射器
    private final JsonIdFieldMapper idFieldMapper;

    // 用于处理文档类型的JSON字段映射器
    private final JsonTypeFieldMapper typeFieldMapper;

    // 用于处理文档原始JSON源数据的JSON字段映射器
    private final JsonSourceFieldMapper sourceFieldMapper;

    // 用于处理文档提升因子（boost）的JSON字段映射器
    private final JsonBoostFieldMapper boostFieldMapper;

    // 根对象的JSON映射器，处理整个JSON文档的映射
    private final JsonObjectMapper rootObjectMapper;

    // 索引时使用的分析器，用于文档索引时的文本处理
    private final Analyzer indexAnalyzer;

    // 搜索时使用的分析器，用于文档搜索时的文本处理
    private final Analyzer searchAnalyzer;

    // 文档字段映射器，包含文档中所有字段的映射信息
    private volatile DocumentFieldMappers fieldMappers;

    // 字段映射监听器列表，用于在字段映射更新时接收通知
    private final List<FieldMapperListener> fieldMapperListeners = newArrayList();

    // 互斥锁，用于同步操作，保证线程安全
    private final Object mutex = new Object();

    public JsonDocumentMapper(JsonObjectMapper rootObjectMapper,
                              JsonUidFieldMapper uidFieldMapper,
                              JsonIdFieldMapper idFieldMapper,
                              JsonTypeFieldMapper typeFieldMapper,
                              JsonSourceFieldMapper sourceFieldMapper,
                              Analyzer indexAnalyzer, Analyzer searchAnalyzer,
                              @Nullable JsonBoostFieldMapper boostFieldMapper,
                              @Nullable String mappingSource) {
        this.type = rootObjectMapper.name();
        this.mappingSource = mappingSource;
        this.rootObjectMapper = rootObjectMapper;
        this.uidFieldMapper = uidFieldMapper;
        this.idFieldMapper = idFieldMapper;
        this.typeFieldMapper = typeFieldMapper;
        this.sourceFieldMapper = sourceFieldMapper;
        this.boostFieldMapper = boostFieldMapper;

        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;

        rootObjectMapper.putMapper(idFieldMapper);
        if (boostFieldMapper != null) {
            rootObjectMapper.putMapper(boostFieldMapper);
        }

        final List<FieldMapper> tempFieldMappers = new ArrayList<FieldMapper>();
        // add the basic ones
        tempFieldMappers.add(typeFieldMapper);
        tempFieldMappers.add(sourceFieldMapper);
        tempFieldMappers.add(uidFieldMapper);
        if (boostFieldMapper != null) {
            tempFieldMappers.add(boostFieldMapper);
        }
        // now traverse and get all the statically defined ones
        rootObjectMapper.traverse(new FieldMapperListener() {
            @Override public void fieldMapper(FieldMapper fieldMapper) {
                tempFieldMappers.add(fieldMapper);
            }
        });

        this.fieldMappers = new DocumentFieldMappers(this, tempFieldMappers);
    }

    @Override public String type() {
        return this.type;
    }

    @Override public String mappingSource() {
        return this.mappingSource;
    }

    @Override public UidFieldMapper uidMapper() {
        return this.uidFieldMapper;
    }

    @Override public IdFieldMapper idMapper() {
        return this.idFieldMapper;
    }

    @Override public TypeFieldMapper typeMapper() {
        return this.typeFieldMapper;
    }

    @Override public SourceFieldMapper sourceMapper() {
        return this.sourceFieldMapper;
    }

    @Override public BoostFieldMapper boostMapper() {
        return this.boostFieldMapper;
    }

    @Override public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override public DocumentFieldMappers mappers() {
        return this.fieldMappers;
    }

    @Override public ParsedDocument parse(String source) {
        return parse(null, null, source);
    }

    @Override
    public ParsedDocument parse(String type, String id, String source) {
        // 从缓存中获取JsonParseContext对象
        JsonParseContext jsonContext = cache.get();

        // 如果提供了类型（type）并且与映射器的类型不匹配，则抛出异常
        if (type != null && !type.equals(this.type)) {
            throw new MapperParsingException("Type mismatch, provide type [" + type + "] but mapper is of type [" + this.type + "]");
        }
        // 设置类型为映射器的类型
        type = this.type;

        try {
            // 使用jsonFactory创建JsonParser，准备解析文档源数据
            JsonParser jp = jsonFactory.createJsonParser(new FastStringReader(source));
            // 重置jsonContext，准备解析
            jsonContext.reset(jp, new Document(), type, source);

            // 读取下一个token，应该是JSON对象的开始
            JsonToken token = jp.nextToken();
            if (token != JsonToken.START_OBJECT) {
                throw new MapperException("Malformed json, must start with an object");
            }
            // 继续读取下一个token
            token = jp.nextToken();
            // 检查token是否是字段名，这里期望的是类型（type）字段
            if (token != JsonToken.FIELD_NAME) {
                throw new MapperException("Malformed json, after first object, the type name must exists");
            }
            // 检查当前字段名是否与映射器的类型匹配
            if (!jp.getCurrentName().equals(type)) {
                if (type == null) {
                    throw new MapperException("Json content type [" + jp.getCurrentName() + "] does not match the type of the mapper [" + type + "]");
                }
                // 如果type为null，意味着我们继续解析其他字段
            } else {
                // 如果匹配，继续读取下一个token，这将是我们文档内容的开始
                token = jp.nextToken();
                if (token != JsonToken.START_OBJECT) {
                    throw new MapperException("Malformed json, after type must start with an object");
                }
            }

            // 如果源映射器（sourceFieldMapper）启用，解析源字段
            if (sourceFieldMapper.enabled()) {
                sourceFieldMapper.parse(jsonContext);
            }
            // 如果提供了文档ID，则设置id并解析UID字段
            if (id != null) {
                jsonContext.id(id);
                uidFieldMapper.parse(jsonContext);
            }
            // 解析类型（type）字段
            typeFieldMapper.parse(jsonContext);

            // 解析根对象映射器
            rootObjectMapper.parse(jsonContext);

            // 如果没有提供文档ID，则在文档添加后解析UID字段
            if (id == null) {
                uidFieldMapper.parse(jsonContext);
            }
            // 如果文档ID尚未解析，则将其标记为外部ID并解析ID字段
            if (jsonContext.parsedIdState() != JsonParseContext.ParsedIdState.PARSED) {
                jsonContext.parsedId(JsonParseContext.ParsedIdState.EXTERNAL);
                idFieldMapper.parse(jsonContext);
            }
        } catch (IOException e) {
            // 如果解析过程中发生IO异常，抛出映射解析异常
            throw new MapperParsingException("Failed to parse", e);
        }
        // 返回解析后的文档对象
        return new ParsedDocument(jsonContext.uid(), jsonContext.id(), jsonContext.type(), jsonContext.doc(), source);
    }

    void addFieldMapper(FieldMapper fieldMapper) {
        synchronized (mutex) {
            fieldMappers = fieldMappers.concat(this, fieldMapper);
            for (FieldMapperListener listener : fieldMapperListeners) {
                listener.fieldMapper(fieldMapper);
            }
        }
    }

    @Override public void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting) {
        synchronized (mutex) {
            fieldMapperListeners.add(fieldMapperListener);
            if (includeExisting) {
                fieldMapperListener.fieldMapper(sourceFieldMapper);
                fieldMapperListener.fieldMapper(typeFieldMapper);
                fieldMapperListener.fieldMapper(idFieldMapper);
                fieldMapperListener.fieldMapper(uidFieldMapper);
                rootObjectMapper.traverse(fieldMapperListener);
            }
        }
    }
}
