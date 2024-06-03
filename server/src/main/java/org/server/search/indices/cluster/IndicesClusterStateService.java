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

package org.server.search.indices.cluster;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.ClusterChangedEvent;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterStateListener;
import org.server.search.cluster.action.index.NodeIndexCreatedAction;
import org.server.search.cluster.action.index.NodeIndexDeletedAction;
import org.server.search.cluster.action.shard.ShardStateAction;
import org.server.search.cluster.metadata.IndexMetaData;
import org.server.search.cluster.metadata.MetaData;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.IndexShardRoutingTable;
import org.server.search.cluster.routing.RoutingNode;
import org.server.search.cluster.routing.RoutingTable;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.index.IndexService;
import org.server.search.index.IndexShardAlreadyExistsException;
import org.server.search.index.gateway.IgnoreGatewayRecoveryException;
import org.server.search.index.gateway.IndexShardGatewayService;
import org.server.search.index.mapper.DocumentMapper;
import org.server.search.index.mapper.MapperService;
import org.server.search.index.shard.IndexShard;
import org.server.search.index.shard.IndexShardState;
import org.server.search.index.shard.InternalIndexShard;
import org.server.search.index.shard.recovery.IgnoreRecoveryException;
import org.server.search.index.shard.recovery.RecoveryAction;
import org.server.search.indices.IndicesService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.settings.Settings;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.*;


public class IndicesClusterStateService extends AbstractComponent implements ClusterStateListener, LifecycleComponent<IndicesClusterStateService> {

    // 生命周期管理对象，用于管理服务的启动、停止等状态
    private final Lifecycle lifecycle = new Lifecycle();

    // IndicesService，用于操作和管理索引的服务
    private final IndicesService indicesService;

    // ClusterService，集群服务，用于获取和更新集群状态
    private final ClusterService clusterService;

    // ThreadPool，线程池，用于执行异步任务和并发处理
    private final ThreadPool threadPool;

    // ShardStateAction，分片状态操作，用于处理分片状态变更的逻辑
    private final ShardStateAction shardStateAction;

    // NodeIndexCreatedAction，节点索引创建动作，用于处理索引在节点上创建的事件
    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    // NodeIndexDeletedAction，节点索引删除动作，用于处理索引在节点上删除的事件
    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    @Inject public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService,
                                              ThreadPool threadPool, ShardStateAction shardStateAction,
                                              NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.shardStateAction = shardStateAction;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public IndicesClusterStateService start() throws SearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        clusterService.add(this);
        return this;
    }

    @Override public IndicesClusterStateService stop() throws SearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        clusterService.remove(this);
        return this;
    }

    @Override public void close() throws SearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        // 如果不允许对索引进行更改，则直接返回
        if (!indicesService.changesAllowed())
            return;

        MetaData metaData = event.state().metaData();
        // 首先，遍历所有需要创建的索引并创建它们
        for (final IndexMetaData indexMetaData : metaData) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                // 如果索引不存在，则创建它
                if (logger.isDebugEnabled()) {
                    logger.debug("Index [{}]: Creating", indexMetaData.index());
                }
                indicesService.createIndex(indexMetaData.index(), indexMetaData.settings(), event.state().nodes().localNode().id());
                // 在单独的线程中触发节点索引创建的回调
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        nodeIndexCreatedAction.nodeIndexCreated(indexMetaData.index(), event.state().nodes().localNodeId());
                    }
                });
            }
        }

        // 获取路由表
        RoutingTable routingTable = event.state().routingTable();

        // 获取本地节点的路由节点信息
        RoutingNode routingNodes = event.state().routingNodes().nodesToShards().get(event.state().nodes().localNodeId());
        if (routingNodes != null) {
            // 应用路由节点和路由表的更改
            applyShards(routingNodes, routingTable, event.state().nodes());
        }

        // 遍历所有索引的元数据，更新映射
        for (IndexMetaData indexMetaData : metaData) {
            if (!indicesService.hasIndex(indexMetaData.index())) {
                // 如果索引服务中没有这个索引，跳过映射更新
                continue;
            }
            String index = indexMetaData.index();
            IndexService indexService = indicesService.indexServiceSafe(index);
            MapperService mapperService = indexService.mapperService();
            ImmutableMap<String, String> mappings = indexMetaData.mappings();
            // 目前不支持删除映射
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                String mappingType = entry.getKey();
                String mappingSource = entry.getValue();

                try {
                    // 如果映射类型不存在，则添加映射
                    if (!mapperService.hasMapping(mappingType)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Index [" + index + "] Adding mapping [" + mappingType + "], source [" + mappingSource + "]");
                        }
                        mapperService.add(mappingType, mappingSource);
                    } else {
                        // 如果映射已存在但源不匹配，则更新映射
                        DocumentMapper existingMapper = mapperService.documentMapper(mappingType);
                        if (!mappingSource.equals(existingMapper.mappingSource())) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Index [" + index + "] Updating mapping [" + mappingType + "], source [" + mappingSource + "]");
                            }
                            mapperService.add(mappingType, mappingSource);
                        }
                    }
                } catch (Exception e) {
                    // 如果添加映射失败，记录警告
                    logger.warn("Failed to add mapping [" + mappingType + "], source [" + mappingSource + "]", e);
                }
            }
        }

        // 遍历所有索引服务中的索引，删除那些在元数据中不存在的索引或特定分片
        for (final String index : indicesService.indices()) {
            if (metaData.index(index) == null) {
                // 如果元数据中没有这个索引，则删除它
                if (logger.isDebugEnabled()) {
                    logger.debug("Index [{}]: Deleting", index);
                }
                indicesService.deleteIndex(index);
                // 在单独的线程中触发节点索引删除的回调
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        nodeIndexDeletedAction.nodeIndexDeleted(index, event.state().nodes().localNodeId());
                    }
                });
            } else if (routingNodes != null) {
                // 如果路由节点信息不为空，删除那些在新的路由节点信息中不存在的分片
                Set<Integer> newShardIds = newHashSet();
                for (final ShardRouting shardRouting : routingNodes) {
                    if (shardRouting.index().equals(index)) {
                        newShardIds.add(shardRouting.id());
                    }
                }
                final IndexService indexService = indicesService.indexService(index);
                if (indexService == null) {
                    continue;
                }
                for (Integer existingShardId : indexService.shardIds()) {
                    if (!newShardIds.contains(existingShardId)) {
                        // 如果现有的分片ID不在新的分片ID集合中，则删除该分片
                        if (logger.isDebugEnabled()) {
                            logger.debug("Index [{}]: Deleting shard [{}]", index, existingShardId);
                        }
                        indexService.deleteShard(existingShardId);
                    }
                }
            }
        }
    }

    private void applyShards(final RoutingNode routingNodes, final RoutingTable routingTable, final Nodes nodes) throws SearchException {
        // 如果不允许对索引进行更改，则直接返回
        if (!indicesService.changesAllowed())
            return;

        // 遍历路由节点中所有的分片路由
        for (final ShardRouting shardRouting : routingNodes) {
            // 安全地获取当前分片所属的索引服务
            final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());

            // 获取当前分片的ID
            final int shardId = shardRouting.id();

            // 如果索引服务中不存在该分片，并且集群主节点认为该分片已启动，则记录错误并标记分片为失败
            if (!indexService.hasShard(shardId) && shardRouting.started()) {
                logger.warn("[" + shardRouting.index() + "][" + shardRouting.shardId().id() + "] Master " + nodes.masterNode() +
                        " marked shard as started, but shard have not been created, mark shard as failed");
                shardStateAction.shardFailed(shardRouting);
                continue;
            }

            // 如果索引服务中存在该分片
            if (indexService.hasShard(shardId)) {
                // 将索引服务的分片转换为InternalIndexShard类型
                InternalIndexShard indexShard = (InternalIndexShard) indexService.shard(shardId);
                // 如果当前分片路由与索引分片的路由不一致
                if (!shardRouting.equals(indexShard.routingEntry())) {
                    // 更新索引分片的路由
                    indexShard.routingEntry(shardRouting);
                    // 通知分片网关服务路由状态已改变
                    indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class).routingStateChanged();
                }
            }

            // 如果分片处于初始化状态
            if (shardRouting.initializing()) {
                // 应用初始化分片的逻辑
                applyInitializingShard(routingTable, nodes, shardRouting);
            }
        }
    }

    private void applyInitializingShard(final RoutingTable routingTable, final Nodes nodes, final ShardRouting shardRouting) throws SearchException {
        // 安全地获取当前分片所属的索引服务
        final IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
        final int shardId = shardRouting.id();
        // 如果索引服务中存在该分片
        if (indexService.hasShard(shardId)) {
            IndexShard indexShard = indexService.shardSafe(shardId);
            if (indexShard.state() == IndexShardState.STARTED) {
                // the master thinks we are initializing, but we are already started
                // (either master failover, or a cluster event before we managed to tell the master we started), mark us as started
                if (logger.isTraceEnabled()) {
                    logger.trace("[" + shardRouting.index() + "][" + shardRouting.shardId().id() + "] Master " + nodes.masterNode() + " marked shard as initializing, but shard already started, mark shard as started");
                }
                shardStateAction.shardStarted(shardRouting);
                return;
            } else {
                if (indexShard.ignoreRecoveryAttempt()) {
                    return;
                }
            }
        }
        // 如果没有分片，则创建它
        if (!indexService.hasShard(shardId)) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Index [{}]: Creating shard [{}]", shardRouting.index(), shardId);
                }
                InternalIndexShard indexShard = (InternalIndexShard) indexService.createShard(shardId);
                indexShard.routingEntry(shardRouting);
            } catch (IndexShardAlreadyExistsException e) {
                // ignore this, the method call can happen several times
            } catch (Exception e) {
                logger.warn("Failed to create shard for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e);
                try {
                    indexService.deleteShard(shardId);
                } catch (Exception e1) {
                    logger.warn("Failed to delete shard after failed creation for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e1);
                }
                shardStateAction.shardFailed(shardRouting);
                return;
            }
        }
        final InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(shardId);
        // 如果分片忽略恢复尝试，则直接返回
        if (indexShard.ignoreRecoveryAttempt()) {
            // we are already recovering (we can get to this state since the cluster event can happen several
            // times while we recover)
            return;
        }
        // 在线程池中执行恢复逻辑
        threadPool.execute(new Runnable() {
            @Override public void run() {
                // recheck here, since the cluster event can be called
                if (indexShard.ignoreRecoveryAttempt()) {
                    return;
                }
                try {
                    RecoveryAction recoveryAction = indexService.shardInjector(shardId).getInstance(RecoveryAction.class);
                    if (!shardRouting.primary()) {
                        // recovery from primary
                        IndexShardRoutingTable shardRoutingTable = routingTable.index(shardRouting.index()).shard(shardRouting.id());
                        for (ShardRouting entry : shardRoutingTable) {
                            if (entry.primary() && entry.started()) {
                                // only recover from started primary, if we can't find one, we will do it next round
                                Node node = nodes.get(entry.currentNodeId());
                                try {
                                    // we are recovering a backup from a primary, so no need to mark it as relocated
                                    recoveryAction.startRecovery(nodes.localNode(), node, false);
                                    shardStateAction.shardStarted(shardRouting);
                                } catch (IgnoreRecoveryException e) {
                                    // that's fine, since we might be called concurrently, just ignore this
                                    break;
                                }
                                break;
                            }
                        }
                    } else {
                        if (shardRouting.relocatingNodeId() == null) {
                            // we are the first primary, recover from the gateway
                            IndexShardGatewayService shardGatewayService = indexService.shardInjector(shardId).getInstance(IndexShardGatewayService.class);
                            try {
                                shardGatewayService.recover();
                                shardStateAction.shardStarted(shardRouting);
                            } catch (IgnoreGatewayRecoveryException e) {
                                // that's fine, we might be called concurrently, just ignore this, we already recovered
                            }
                        } else {
                            // relocating primaries, recovery from the relocating shard
                            Node node = nodes.get(shardRouting.relocatingNodeId());
                            try {
                                // we mark the primary we are going to recover from as relocated
                                recoveryAction.startRecovery(nodes.localNode(), node, true);
                                shardStateAction.shardStarted(shardRouting);
                            } catch (IgnoreRecoveryException e) {
                                // that's fine, since we might be called concurrently, just ignore this, we are already recovering
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to start shard for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e);
                    if (indexService.hasShard(shardId)) {
                        try {
                            indexService.deleteShard(shardId);
                        } catch (Exception e1) {
                            logger.warn("Failed to delete shard after failed startup for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e1);
                        }
                    }
                    try {
                        shardStateAction.shardFailed(shardRouting);
                    } catch (Exception e1) {
                        logger.warn("Failed to mark shard as failed after a failed start for index [" + indexService.index().name() + "] and shard id [" + shardRouting.id() + "]", e);
                    }
                }
            }
        });
    }
}
