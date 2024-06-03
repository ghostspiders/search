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

package org.server.search.index.mapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.server.search.env.Environment;
import org.server.search.env.FailedToResolveConfigException;
import org.server.search.index.AbstractIndexComponent;
import org.server.search.index.Index;
import org.server.search.index.IndexLifecycle;
import org.server.search.index.analysis.AnalysisService;
import org.server.search.index.mapper.json.JsonDocumentMapperParser;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.Nullable;
import org.server.search.util.concurrent.ThreadSafe;
import org.server.search.util.io.Streams;
import org.server.search.util.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;

import static org.server.search.util.MapBuilder.*;


@IndexLifecycle
@ThreadSafe
public class MapperService extends AbstractIndexComponent implements Iterable<DocumentMapper> {


    /**
     * 如果在仓库中尚不存在，则会自动创建类型。
     */
    private final boolean dynamic;

    /**
     * 动态映射的文件存储位置。
     */
    private final String dynamicMappingLocation;

    /**
     * 动态映射文件的URL。
     */
    private final URL dynamicMappingUrl;

    /**
     * 用于加载索引相关类的类加载器。
     */
    private final ClassLoader indexClassLoader;

    /**
     * 动态映射源代码。
     */
    private final String dynamicMappingSource;

    /**
     * 存储映射信息的不可变映射，键为类型名称，值为DocumentMapper。
     */
    private volatile ImmutableMap<String, DocumentMapper> mappers = ImmutableMap.of();

    /**
     * 同步锁，用于确保映射更新的线程安全。
     */
    private final Object mutex = new Object();

    /**
     * 根据名称映射的字段映射器的不可变映射。
     */
    private volatile ImmutableMap<String, FieldMappers> nameFieldMappers = ImmutableMap.of();

    /**
     * 根据索引名称映射的字段映射器的不可变映射。
     */
    private volatile ImmutableMap<String, FieldMappers> indexNameFieldMappers = ImmutableMap.of();

    /**
     * 根据完整名称映射的字段映射器的不可变映射。
     */
    private volatile ImmutableMap<String, FieldMappers> fullNameFieldMappers = ImmutableMap.of();

    /**
     * ID字段映射器集合。
     */
    private volatile FieldMappers idFieldMappers = new FieldMappers();

    /**
     * 类型字段映射器集合。
     */
    private volatile FieldMappers typeFieldMappers = new FieldMappers();

    /**
     * 唯一标识字段映射器集合。
     */
    private volatile FieldMappers uidFieldMappers = new FieldMappers();

    /**
     * 源字段映射器集合。
     */
    private volatile FieldMappers sourceFieldMappers = new FieldMappers();

    /**
     * 文档映射解析器，目前只使用JSON格式的解析器。
     * 可以扩展以支持自定义解析器。
     */
    private final DocumentMapperParser documentParser;

    /**
     * 内部字段映射监听器，用于处理字段映射的变更。
     */
    private final InternalFieldMapperListener fieldMapperListener = new InternalFieldMapperListener();

    /**
     * 智能索引名称搜索分析器，用于优化搜索分析。
     */
    private final SmartIndexNameSearchAnalyzer searchAnalyzer;

    /**
     * 构造函数，初始化映射服务。
     * @param index 当前索引的相关信息。
     * @param indexSettings 与索引相关的设置。
     * @param environment 环境配置，提供配置文件解析等。
     * @param analysisService 分析服务，提供分析器相关功能。
     */
    @Inject
    public MapperService(Index index, @IndexSettings Settings indexSettings, Environment environment, AnalysisService analysisService) {
        super(index, indexSettings);
        this.documentParser = new JsonDocumentMapperParser(analysisService); // 初始化JSON文档映射解析器
        this.searchAnalyzer = new SmartIndexNameSearchAnalyzer(analysisService.defaultSearchAnalyzer()); // 初始化智能索引名称搜索分析器
        this.indexClassLoader = indexSettings.getClassLoader(); // 获取索引的类加载器

        this.dynamic = componentSettings.getAsBoolean("dynamic", true); // 是否动态创建类型，默认为true
        String dynamicMappingLocation = componentSettings.get("dynamicMappingLocation"); // 动态映射文件的位置
        URL dynamicMappingUrl;
        if (dynamicMappingLocation == null) {
            try {
                dynamicMappingUrl = environment.resolveConfig("dynamic-mapping.json"); // 从环境配置中解析动态映射配置文件URL
            } catch (FailedToResolveConfigException e) {
                // 如果解析失败，使用内置的动态映射配置文件
                dynamicMappingUrl = indexClassLoader.getResource("org/server/search/index/mapper/json/dynamic-mapping.json");
            }
        } else {
            try {
                dynamicMappingUrl = environment.resolveConfig(dynamicMappingLocation); // 解析指定位置的动态映射配置文件URL
            } catch (FailedToResolveConfigException e) {
                // 如果解析失败，尝试将文件路径转换为URL
                try {
                    dynamicMappingUrl = new File(dynamicMappingLocation).toURI().toURL();
                } catch (MalformedURLException e1) {
                    throw new FailedToResolveConfigException("Failed to resolve dynamic mapping location [" + dynamicMappingLocation + "]");
                }
            }
        }
        this.dynamicMappingUrl = dynamicMappingUrl; // 保存动态映射URL
        this.dynamicMappingLocation = (dynamicMappingLocation == null) ? dynamicMappingUrl.toExternalForm() : dynamicMappingLocation; // 保存动态映射位置

        if (dynamic) {
            try {
                // 如果启用动态映射，从URL加载默认映射源代码
                dynamicMappingSource = Streams.copyToString(new InputStreamReader(dynamicMappingUrl.openStream(), "UTF8"));
            } catch (IOException e) {
                throw new MapperException("Failed to load default mapping source from [" + dynamicMappingLocation + "]", e);
            }
        } else {
            dynamicMappingSource = null; // 如果不启用动态映射，则为null
        }
        logger.debug("Using dynamic [{}] with location [{}] and source [{}]", new Object[]{dynamic, dynamicMappingLocation, dynamicMappingSource}); // 日志记录动态映射配置信息
    }

    @Override public UnmodifiableIterator<DocumentMapper> iterator() {
        return mappers.values().iterator();
    }

    /**
     * 根据类型名称获取DocumentMapper。
     * 如果映射存在，则返回它；如果不存在且动态创建未启用，则返回null。
     * 如果动态创建启用，并且映射不存在，则会尝试动态创建映射。
     * @param type 类型名称
     * @return DocumentMapper实例，如果不存在则可能返回null
     */
    public DocumentMapper type(String type) {
        DocumentMapper mapper = mappers.get(type); // 从映射中获取DocumentMapper
        if (mapper != null) {
            return mapper; // 如果找到了映射，则返回
        }
        if (!dynamic) {
            return null; // 如果动态创建未启用，则返回null
        }
        // 尝试动态创建映射
        synchronized (mutex) { // 同步锁，确保线程安全
            mapper = mappers.get(type); // 再次检查映射是否存在，可能在其他线程中已创建
            if (mapper != null) {
                return mapper; // 如果已创建，则返回
            }
            add(type, dynamicMappingSource); // 调用add方法动态创建映射
            return mappers.get(type); // 返回新创建的映射
        }
    }

    /**
     * 添加或更新映射定义。
     * 根据提供的类型和映射源字符串，使用DocumentMapperParser解析并添加映射。
     * @param type 类型名称
     * @param mappingSource 映射源字符串
     */
    public void add(String type, String mappingSource) {
        add(documentParser.parse(type, mappingSource)); // 使用解析器解析映射定义并添加
    }

    /**
     * 添加或更新映射定义。
     * 根据提供的映射源字符串，使用DocumentMapperParser解析并添加映射。
     * @param mappingSource 映射源字符串
     * @throws MapperParsingException 如果映射解析失败
     */
    public void add(String mappingSource) throws MapperParsingException {
        add(documentParser.parse(mappingSource)); // 使用解析器解析映射定义并添加
    }

    /**
     * Just parses and returns the mapper without adding it.
     */
    public DocumentMapper parse(String mappingType, String mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource);
    }

    public boolean hasMapping(String mappingType) {
        return mappers.containsKey(mappingType);
    }

    public DocumentMapper documentMapper(String type) {
        return mappers.get(type);
    }

    public FieldMappers idFieldMappers() {
        return this.idFieldMappers;
    }

    public FieldMappers typeFieldMappers() {
        return this.typeFieldMappers;
    }

    public FieldMappers sourceFieldMappers() {
        return this.sourceFieldMappers;
    }

    public FieldMappers uidFieldMappers() {
        return this.uidFieldMappers;
    }

    /**
     * Returns {@link FieldMappers} for all the {@link FieldMapper}s that are registered
     * under the given name across all the different {@link DocumentMapper} types.
     *
     * @param name The name to return all the {@link FieldMappers} for across all {@link DocumentMapper}s.
     * @return All the {@link FieldMappers} for across all {@link DocumentMapper}s
     */
    public FieldMappers name(String name) {
        return nameFieldMappers.get(name);
    }

    /**
     * Returns {@link FieldMappers} for all the {@link FieldMapper}s that are registered
     * under the given indexName across all the different {@link DocumentMapper} types.
     *
     * @param indexName The indexName to return all the {@link FieldMappers} for across all {@link DocumentMapper}s.
     * @return All the {@link FieldMappers} across all {@link DocumentMapper}s for the given indexName.
     */
    public FieldMappers indexName(String indexName) {
        return indexNameFieldMappers.get(indexName);
    }

    /**
     * Returns the {@link FieldMappers} of all the {@link FieldMapper}s that are
     * registered under the give fullName ({@link FieldMapper#fullName()} across
     * all the different {@link DocumentMapper} types.
     *
     * @param fullName The full name
     * @return All teh {@link FieldMappers} across all the {@link DocumentMapper}s for the given fullName.
     */
    public FieldMappers fullName(String fullName) {
        return fullNameFieldMappers.get(fullName);
    }

    /**
     * 根据智能名称获取字段映射器。
     * 此方法与{@link #smartName(String)}相同，但它只返回字段映射器。
     * @param smartName 智能名称，可能包含类型和字段名称，格式为"type.name"
     * @return 字段映射器，如果找不到则返回null
     */
    public FieldMappers smartNameFieldMappers(String smartName) {
        int dotIndex = smartName.indexOf('.'); // 查找第一个点的位置
        if (dotIndex != -1) {
            // 如果存在点，则尝试解析类型和字段名称
            String possibleType = smartName.substring(0, dotIndex); // 类型为点之前的部分
            DocumentMapper possibleDocMapper = mappers.get(possibleType); // 根据类型获取DocumentMapper
            if (possibleDocMapper != null) {
                // 如果找到DocumentMapper，则尝试获取字段映射器
                String possibleName = smartName.substring(dotIndex + 1); // 字段名称为点之后的部分
                FieldMappers mappers = possibleDocMapper.mappers().fullName(possibleName); // 尝试通过完整名称获取字段映射器
                if (mappers != null) {
                    return mappers; // 如果找到，则返回
                }
                mappers = possibleDocMapper.mappers().indexName(possibleName); // 尝试通过索引名称获取字段映射器
                if (mappers != null) {
                    return mappers; // 如果找到，则返回
                }
            }
        }
        // 如果智能名称中没有点，或者上面的尝试失败，则尝试通过完整名称获取字段映射器
        FieldMappers mappers = fullName(smartName);
        if (mappers != null) {
            return mappers; // 如果通过完整名称找到，则返回
        }
        // 最后，尝试通过索引名称获取字段映射器
        return indexName(smartName); // 返回通过索引名称找到的字段映射器，如果找不到则返回null
    }

    /**
     * 根据智能名称获取SmartNameFieldMappers对象。
     * 智能名称是一个字符串，可能包含类型和字段名称，格式为"type.name"。
     * @param smartName 智能名称
     * @return SmartNameFieldMappers对象，如果找不到则返回null
     */
    public SmartNameFieldMappers smartName(String smartName) {
        int dotIndex = smartName.indexOf('.'); // 查找智能名称中的第一个点的位置
        if (dotIndex != -1) {
            // 如果智能名称包含点，则可能包含类型和字段名称
            String possibleType = smartName.substring(0, dotIndex); // 类型名称为点之前的部分
            DocumentMapper possibleDocMapper = mappers.get(possibleType); // 根据类型名称获取DocumentMapper
            if (possibleDocMapper != null) {
                // 如果找到了DocumentMapper，尝试获取字段映射器
                String possibleName = smartName.substring(dotIndex + 1); // 字段名称为点之后的部分
                FieldMappers mappers = possibleDocMapper.mappers().fullName(possibleName); // 尝试通过完整名称获取字段映射器
                if (mappers != null) {
                    // 如果找到了字段映射器，创建并返回SmartNameFieldMappers对象
                    return new SmartNameFieldMappers(mappers, possibleDocMapper);
                }
                mappers = possibleDocMapper.mappers().indexName(possibleName); // 尝试通过索引名称获取字段映射器
                if (mappers != null) {
                    // 如果找到了字段映射器，创建并返回SmartNameFieldMappers对象
                    return new SmartNameFieldMappers(mappers, possibleDocMapper);
                }
            }
        }
        // 如果智能名称中没有点，或者根据类型和字段名称没有找到映射器
        FieldMappers fieldMappers = fullName(smartName); // 尝试通过完整名称获取字段映射器
        if (fieldMappers != null) {
            // 如果找到了字段映射器，创建并返回SmartNameFieldMappers对象
            return new SmartNameFieldMappers(fieldMappers, null);
        }
        fieldMappers = indexName(smartName); // 尝试通过索引名称获取字段映射器
        if (fieldMappers != null) {
            // 如果找到了字段映射器，创建并返回SmartNameFieldMappers对象
            return new SmartNameFieldMappers(fieldMappers, null);
        }
        // 如果所有尝试都失败，则返回null
        return null;
    }

    /**
     * 添加一个DocumentMapper到映射服务中。
     * 这个方法是线程安全的，它同步添加操作以避免并发问题。
     * @param mapper 要添加的DocumentMapper实例
     */
    public void add(DocumentMapper mapper) {
        synchronized (mutex) { // 使用mutex进行同步，确保线程安全
            // 检查映射类型名称是否以'_'开头，这是不允许的
            if (mapper.type().charAt(0) == '_') {
                throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
            }
            // 将新的DocumentMapper添加到映射中
            mappers = newMapBuilder(mappers).put(mapper.type(), mapper).immutableMap();
            // 为DocumentMapper添加字段映射监听器
            mapper.addFieldMapperListener(fieldMapperListener, true);
        }
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public static class SmartNameFieldMappers {
        private final FieldMappers fieldMappers;
        private final DocumentMapper docMapper;

        public SmartNameFieldMappers(FieldMappers fieldMappers, @Nullable DocumentMapper docMapper) {
            this.fieldMappers = fieldMappers;
            this.docMapper = docMapper;
        }

        public FieldMappers fieldMappers() {
            return fieldMappers;
        }

        public boolean hasDocMapper() {
            return docMapper != null;
        }

        public DocumentMapper docMapper() {
            return docMapper;
        }
    }

    private class SmartIndexNameSearchAnalyzer extends Analyzer {

        private final Analyzer defaultAnalyzer;

        private SmartIndexNameSearchAnalyzer(Analyzer defaultAnalyzer) {
            this.defaultAnalyzer = defaultAnalyzer;
        }

        @Override
        protected TokenStreamComponents createComponents(String s) {
            return null;
        }

        public TokenStream tokenStream1(String fieldName, Reader reader) {
            int dotIndex = fieldName.indexOf('.');
            if (dotIndex != -1) {
                String possibleType = fieldName.substring(0, dotIndex);
                DocumentMapper possibleDocMapper = mappers.get(possibleType);
                if (possibleDocMapper != null) {
                    return possibleDocMapper.mappers().searchAnalyzer().tokenStream(fieldName, reader);
                }
            }
            FieldMappers mappers = indexNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().tokenStream(fieldName, reader);
            }
            return defaultAnalyzer.tokenStream(fieldName, reader);
        }

        public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
            int dotIndex = fieldName.indexOf('.');
            if (dotIndex != -1) {
                String possibleType = fieldName.substring(0, dotIndex);
                DocumentMapper possibleDocMapper = mappers.get(possibleType);
                if (possibleDocMapper != null) {
                    return possibleDocMapper.mappers().searchAnalyzer().tokenStream(fieldName, reader);
                }
            }
            FieldMappers mappers = indexNameFieldMappers.get(fieldName);
            if (mappers != null && mappers.mapper() != null && mappers.mapper().searchAnalyzer() != null) {
                return mappers.mapper().searchAnalyzer().tokenStream(fieldName, reader);
            }
            return defaultAnalyzer.tokenStream(fieldName, reader);
        }
    }

    /**
     * 内部类，实现了FieldMapperListener接口，用于监听字段映射器的变化。
     */
    private class InternalFieldMapperListener implements FieldMapperListener {

        /**
         * 当字段映射器发生变化时调用此方法。
         * @param fieldMapper 发生变化的字段映射器
         */
        @Override
        public void fieldMapper(FieldMapper fieldMapper) {
            synchronized (mutex) { // 使用mutex进行同步，确保线程安全
                // 根据字段映射器的类型，更新对应的字段映射器集合
                if (fieldMapper instanceof IdFieldMapper) {
                    idFieldMappers = idFieldMappers.concat(fieldMapper);
                }
                if (fieldMapper instanceof TypeFieldMapper) {
                    typeFieldMappers = typeFieldMappers.concat(fieldMapper);
                }
                if (fieldMapper instanceof SourceFieldMapper) {
                    sourceFieldMappers = sourceFieldMappers.concat(fieldMapper);
                }
                if (fieldMapper instanceof UidFieldMapper) {
                    uidFieldMappers = uidFieldMappers.concat(fieldMapper);
                }

                // 更新nameFieldMappers映射
                FieldMappers mappers = nameFieldMappers.get(fieldMapper.name());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                nameFieldMappers = newMapBuilder(nameFieldMappers).put(fieldMapper.name(), mappers).immutableMap();

                // 更新indexNameFieldMappers映射
                mappers = indexNameFieldMappers.get(fieldMapper.indexName());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                indexNameFieldMappers = newMapBuilder(indexNameFieldMappers).put(fieldMapper.indexName(), mappers).immutableMap();

                // 更新fullNameFieldMappers映射
                mappers = fullNameFieldMappers.get(fieldMapper.indexName());
                if (mappers == null) {
                    mappers = new FieldMappers(fieldMapper);
                } else {
                    mappers = mappers.concat(fieldMapper);
                }
                fullNameFieldMappers = newMapBuilder(fullNameFieldMappers).put(fieldMapper.fullName(), mappers).immutableMap();
            }
        }
    }
}
