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

package org.server.search.cluster.metadata;

import com.google.common.collect.ImmutableMap;
import org.server.search.util.MapBuilder;
import org.server.search.util.Preconditions;
import org.server.search.util.concurrent.Immutable;
import org.server.search.util.settings.ImmutableSettings;
import org.server.search.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import static org.server.search.util.settings.ImmutableSettings.*;


@Immutable
public class IndexMetaData {

    // 索引设置中分片数量的键
    public static final String SETTING_NUMBER_OF_SHARDS = "index.numberOfShards";
    // 索引设置中副本数量的键
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.numberOfReplicas";

    // 索引的名称
    private final String index;
    // 索引的配置信息
    private final Settings settings;
    // 索引的映射信息，是一个键值对集合，键是映射类型，值是映射定义
    private final ImmutableMap<String, String> mappings;
    // 索引的总分片数量，包括主分片和副本分片
    private transient final int totalNumberOfShards;

    // 构造方法，初始化索引元数据
    private IndexMetaData(String index, Settings settings, ImmutableMap<String, String> mappings) {
        // 确保设置了分片数和副本数
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1) != -1, "must specify numberOfShards");
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1) != -1, "must specify numberOfReplicas");
        this.index = index;
        this.settings = settings;
        this.mappings = mappings;
        // 计算总分片数
        this.totalNumberOfShards = numberOfShards() * (numberOfReplicas() + 1);
    }

    // 获取索引名称
    public String index() {
        return index;
    }

    // 获取分片数
    public int numberOfShards() {
        return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
    }

    // 获取副本数
    public int numberOfReplicas() {
        return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
    }

    // 获取总分片数
    public int totalNumberOfShards() {
        return totalNumberOfShards;
    }

    // 获取索引的配置信息
    public Settings settings() {
        return settings;
    }

    // 获取索引的映射信息
    public ImmutableMap<String, String> mappings() {
        return mappings;
    }

    // 静态方法，使用索引名称创建一个新的IndexMetaData构建器
    public static Builder newIndexMetaDataBuilder(String index) {
        return new Builder(index);
    }

    // 静态方法，使用现有的IndexMetaData创建一个新的IndexMetaData构建器
    public static Builder newIndexMetaDataBuilder(IndexMetaData indexMetaData) {
        return new Builder(indexMetaData);
    }
    public static class Builder {
        // 索引名称
        private String index;
        // 索引的设置
        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        // 索引的映射定义，使用MapBuilder来构建
        private MapBuilder<String, String> mappings = MapBuilder.newMapBuilder();

        // 构造函数，初始化索引名称
        public Builder(String index) {
            this.index = index;
        }

        // 构造函数，初始化IndexMetaData对象
        public Builder(IndexMetaData indexMetaData) {
            this(indexMetaData.index());
            settings(indexMetaData.settings());
            mappings.putAll(indexMetaData.mappings);
        }

        // 获取索引名称
        public String index() {
            return index;
        }

        // 设置分片数并返回构建器自身
        public Builder numberOfShards(int numberOfShards) {
            settings = ImmutableSettings.settingsBuilder().putAll(settings).putInt(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            return this;
        }

        // 获取分片数
        public int numberOfShards() {
            return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
        }

        // 设置副本数并返回构建器自身
        public Builder numberOfReplicas(int numberOfReplicas) {
            settings = ImmutableSettings.settingsBuilder().putAll(settings).putInt(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            return this;
        }

        // 获取副本数
        public int numberOfReplicas() {
            return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
        }

        // 设置索引的配置信息并返回构建器自身
        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        // 从映射中移除指定类型并返回构建器自身
        public Builder removeMapping(String mappingType) {
            mappings.remove(mappingType);
            return this;
        }

        // 添加映射定义并返回构建器自身
        public Builder addMapping(String mappingType, String mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        // 构建并返回IndexMetaData实例
        public IndexMetaData build() {
            return new IndexMetaData(index, settings, mappings.immutableMap());
        }

        // 从DataInput中读取数据，创建并返回IndexMetaData实例
        public static IndexMetaData readFrom(DataInput in, Settings globalSettings) throws ClassNotFoundException, IOException {
            Builder builder = new Builder(in.readUTF());
            builder.settings(readSettingsFromStream(in, globalSettings));
            int mappingsSize = in.readInt();
            for (int i = 0; i < mappingsSize; i++) {
                builder.addMapping(in.readUTF(), in.readUTF());
            }
            return builder.build();
        }

        // 将IndexMetaData实例写入DataOutput
        public static void writeTo(IndexMetaData indexMetaData, DataOutput out) throws IOException {
            out.writeUTF(indexMetaData.index());
            writeSettingsToStream(indexMetaData.settings(), out);
            out.writeInt(indexMetaData.mappings().size());
            for (Map.Entry<String, String> entry : indexMetaData.mappings().entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
    }
}
