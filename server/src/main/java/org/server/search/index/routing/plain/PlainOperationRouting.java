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

package org.server.search.index.routing.plain;

import com.google.inject.Inject;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.metadata.IndexMetaData;
import org.server.search.cluster.routing.GroupShardsIterator;
import org.server.search.cluster.routing.IndexRoutingTable;
import org.server.search.cluster.routing.IndexShardRoutingTable;
import org.server.search.cluster.routing.ShardsIterator;
import org.server.search.index.AbstractIndexComponent;
import org.server.search.index.Index;
import org.server.search.index.IndexShardMissingException;
import org.server.search.index.routing.OperationRouting;
import org.server.search.index.routing.hash.HashFunction;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.ShardId;
import org.server.search.indices.IndexMissingException;
import org.server.search.util.IdentityHashSet;
import org.server.search.util.Nullable;
import org.server.search.util.settings.Settings;

 
public class PlainOperationRouting extends AbstractIndexComponent implements OperationRouting {

    private final HashFunction hashFunction;

    @Inject public PlainOperationRouting(Index index, @IndexSettings Settings indexSettings, HashFunction hashFunction) {
        super(index, indexSettings);
        this.hashFunction = hashFunction;
    }

    @Override public ShardsIterator indexShards(ClusterState clusterState, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, type, id).shardsIt();
    }

    @Override public ShardsIterator deleteShards(ClusterState clusterState, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, type, id).shardsIt();
    }

    @Override public ShardsIterator getShards(ClusterState clusterState, String type, String id) throws IndexMissingException, IndexShardMissingException {
        return shards(clusterState, type, id).shardsRandomIt();
    }

    @Override public GroupShardsIterator deleteByQueryShards(ClusterState clusterState) throws IndexMissingException {
        return indexRoutingTable(clusterState).groupByShardsIt();
    }

    @Override public GroupShardsIterator searchShards(ClusterState clusterState, @Nullable String queryHint) throws IndexMissingException {
        IdentityHashSet<ShardsIterator> set = new IdentityHashSet<ShardsIterator>();
        IndexRoutingTable indexRouting = indexRoutingTable(clusterState);
        for (IndexShardRoutingTable indexShard : indexRouting) {
            set.add(indexShard.shardsRandomIt());
        }
        return new GroupShardsIterator(set);
    }

    public IndexMetaData indexMetaData(ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index.name());
        if (indexMetaData == null) {
            throw new IndexMissingException(index);
        }
        return indexMetaData;
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index.name());
        if (indexRouting == null) {
            throw new IndexMissingException(index);
        }
        return indexRouting;
    }


    protected IndexShardRoutingTable shards(ClusterState clusterState, String type, String id) {
        // 使用hash函数计算文档ID和类型的哈希值，然后取绝对值
        // 然后使用模运算确定分片ID
        int shardId = Math.abs(hash(type, id)) % indexMetaData(clusterState).numberOfShards();

        // 根据集群状态获取索引的路由表
        IndexShardRoutingTable indexShard = indexRoutingTable(clusterState).shard(shardId);

        // 如果没有找到对应的分片，抛出异常
        if (indexShard == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }

        // 返回找到的分片路由表
        return indexShard;
    }

    protected int hash(String type, String id) {
        return hashFunction.hash(type, id);
    }
}
