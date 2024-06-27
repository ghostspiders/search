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
import com.google.common.collect.UnmodifiableIterator;
import org.server.search.util.MapBuilder;
import org.server.search.util.Nullable;
import org.server.search.util.concurrent.Immutable;
import org.server.search.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.server.search.util.MapBuilder.*;


@Immutable
public class MetaData implements Iterable<IndexMetaData> {

    // 定义一个空的元数据对象，作为静态常量
    public static MetaData EMPTY_META_DATA = newMetaDataBuilder().build();
    public static final String UNKNOWN_CLUSTER_UUID = "_na_";

    // 存储索引名称和对应的IndexMetaData对象的不可变Map
    private final ImmutableMap<String, IndexMetaData> indices;

    // 限制每个节点上分片的最大数量
    private final int maxNumberOfShardsPerNode;

    // 所有索引的总分片数，是一个瞬时变量，不会被序列化
    private final transient int totalNumberOfShards;
    private final CoordinationMetaData coordinationMetaData;
    private final String clusterUUID;
    private final boolean clusterUUIDCommitted;
    // MetaData类的构造函数
    private MetaData(ImmutableMap<String, IndexMetaData> indices, int maxNumberOfShardsPerNode, String clusterUUID, boolean clusterUUIDCommitted, CoordinationMetaData coordinationMetaData) {
        // 使用ImmutableMap的copyOf方法来创建一个不可变副本
        this.indices = ImmutableMap.copyOf(indices);
        this.maxNumberOfShardsPerNode = maxNumberOfShardsPerNode;
        // 计算总分片数
        int totalNumberOfShards = 0;
        for (IndexMetaData indexMetaData : indices.values()) {
            totalNumberOfShards += indexMetaData.totalNumberOfShards();
        }
        this.totalNumberOfShards = totalNumberOfShards;
        this.coordinationMetaData = coordinationMetaData;
        this.clusterUUID = clusterUUID;
        this.clusterUUIDCommitted = clusterUUIDCommitted;
    }

    // 检查是否存在指定名称的索引
    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    // 获取指定名称的IndexMetaData对象
    public IndexMetaData index(String index) {
        return indices.get(index);
    }

    // 返回索引的不可变Map
    public ImmutableMap<String, IndexMetaData> indices() {
        return this.indices;
    }

    // 返回每个节点上允许的最大分片数
    public int maxNumberOfShardsPerNode() {
        return this.maxNumberOfShardsPerNode;
    }

    // 返回所有索引的总分片数
    public int totalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    // 实现Iterable接口的iterator方法，返回一个不可修改的迭代器
    @Override
    public UnmodifiableIterator<IndexMetaData> iterator() {
        return indices.values().iterator();
    }

    // 静态方法，用于创建MetaData的构建器
    public static Builder newMetaDataBuilder() {
        return new Builder();
    }

    public CoordinationMetaData coordinationMetaData() {
        return this.coordinationMetaData;
    }
    public String clusterUUID() {
        return this.clusterUUID;
    }
    public boolean clusterUUIDCommitted() {
        return this.clusterUUIDCommitted;
    }
    public static Builder builder(MetaData metaData) {
        return new Builder(metaData);
    }

    // MetaData的构建器内部类
    public static class Builder {

        // 每个节点上分片的最大数量，默认值为100
        private int maxNumberOfShardsPerNode = 100;
        private String clusterUUID;
        private boolean clusterUUIDCommitted;
        private CoordinationMetaData coordinationMetaData = CoordinationMetaData.EMPTY_META_DATA;
        // 用于构建索引Map的构建器
        private MapBuilder<String, IndexMetaData> indices = newMapBuilder();
        public Builder() {
            clusterUUID = UNKNOWN_CLUSTER_UUID;
        }
        public Builder(MetaData metaData) {
            this.clusterUUID = metaData.clusterUUID;
            this.clusterUUIDCommitted = metaData.clusterUUIDCommitted;
            this.coordinationMetaData = metaData.coordinationMetaData;
        }
        public Builder coordinationMetaData(CoordinationMetaData coordinationMetaData) {
            this.coordinationMetaData = coordinationMetaData;
            return this;
        }
        public Builder clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
            return this;
        }

        public Builder clusterUUIDCommitted(boolean clusterUUIDCommitted) {
            this.clusterUUIDCommitted = clusterUUIDCommitted;
            return this;
        }
        // 向构建器添加一个IndexMetaData对象
        public Builder put(IndexMetaData.Builder indexMetaDataBuilder) {
            return put(indexMetaDataBuilder.build());
        }

        // 向构建器添加一个IndexMetaData对象
        public Builder put(IndexMetaData indexMetaData) {
            indices.put(indexMetaData.index(), indexMetaData);
            return this;
        }

        // 从构建器中移除指定索引
        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        // 将另一个MetaData对象的所有索引添加到构建器中
        public Builder metaData(MetaData metaData) {
            indices.putAll(metaData.indices);
            return this;
        }

        // 设置每个节点上分片的最大数量
        public Builder maxNumberOfShardsPerNode(int maxNumberOfShardsPerNode) {
            this.maxNumberOfShardsPerNode = maxNumberOfShardsPerNode;
            return this;
        }

        // 构建并返回MetaData对象
        public MetaData build() {
            return new MetaData(indices.immutableMap(), maxNumberOfShardsPerNode,clusterUUID, clusterUUIDCommitted,coordinationMetaData);
        }

        // 从DataInput读取数据并构建MetaData对象
        public static MetaData readFrom(DataInput in, @Nullable Settings globalSettings) throws IOException, ClassNotFoundException {
            Builder builder = new Builder();
            builder.maxNumberOfShardsPerNode(in.readInt());
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexMetaData.Builder.readFrom(in, globalSettings));
            }
            return builder.build();
        }

        // 将MetaData对象写入DataOutput
        public static void writeTo(MetaData metaData, DataOutput out) throws IOException {
            out.writeInt(metaData.maxNumberOfShardsPerNode());
            out.writeInt(metaData.indices.size());
            for (IndexMetaData indexMetaData : metaData) {
                IndexMetaData.Builder.writeTo(indexMetaData, out);
            }
        }
    }
}