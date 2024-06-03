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

import org.apache.lucene.analysis.Analyzer;
import org.server.search.util.Nullable;
import org.server.search.util.concurrent.ThreadSafe;


@ThreadSafe
public interface DocumentMapper {

    // 获取文档的类型
    String type();

    /**
     * 如果是通过解析映射定义构建的，则返回它。否则，返回null。
     */
    String mappingSource();

    // 获取用于处理文档唯一标识符的字段映射器
    UidFieldMapper uidMapper();

    // 获取用于处理文档ID的字段映射器
    IdFieldMapper idMapper();

    // 获取用于处理文档类型的字段映射器
    TypeFieldMapper typeMapper();

    // 获取用于处理文档源的字段映射器
    SourceFieldMapper sourceMapper();

    // 获取用于处理文档提升因子的字段映射器
    BoostFieldMapper boostMapper();

    // 获取所有文档字段映射器的集合
    DocumentFieldMappers mappers();

    /**
     * 获取默认的索引分析器。
     * 注意，应该使用DocumentFieldMappers#indexAnalyzer()代替。
     */
    Analyzer indexAnalyzer();

    /**
     * 获取默认的搜索分析器。
     * 注意，应该使用DocumentFieldMappers#searchAnalyzer()代替。
     */
    Analyzer searchAnalyzer();

    /**
     * 将源数据解析为解析后的文档。
     * <p>
     * 验证源数据是否具有提供的类型和ID。
     * 注意，大多数时候我们已经有ID和类型，即使它们也存在于源数据中。
     */
    ParsedDocument parse(@Nullable String type, @Nullable String id, String source) throws MapperParsingException;

    /**
     * 将源数据解析为解析后的文档。
     */
    ParsedDocument parse(String source) throws MapperParsingException;

    /**
     * 添加字段映射器监听器。
     * @param fieldMapperListener 要添加的监听器
     * @param includeExisting 是否包括现有的字段映射器
     */
    void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting);
}