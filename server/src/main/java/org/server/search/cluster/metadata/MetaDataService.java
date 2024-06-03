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

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.ClusterStateUpdateTask;
import org.server.search.cluster.action.index.NodeIndexCreatedAction;
import org.server.search.cluster.action.index.NodeIndexDeletedAction;
import org.server.search.cluster.routing.IndexRoutingTable;
import org.server.search.cluster.routing.RoutingTable;
import org.server.search.cluster.routing.strategy.ShardsRoutingStrategy;
import org.server.search.index.Index;
import org.server.search.index.IndexService;
import org.server.search.index.mapper.DocumentMapper;
import org.server.search.index.mapper.InvalidTypeNameException;
import org.server.search.indices.IndexAlreadyExistsException;
import org.server.search.indices.IndexMissingException;
import org.server.search.indices.IndicesService;
import org.server.search.indices.InvalidIndexNameException;
import org.server.search.util.Strings;
import org.server.search.util.TimeValue;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.ImmutableSettings;
import org.server.search.util.settings.Settings;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.server.search.cluster.ClusterState.*;
import static org.server.search.cluster.metadata.IndexMetaData.*;
import static org.server.search.cluster.metadata.MetaData.*;

 
public class MetaDataService extends AbstractComponent {

    // 集群服务，用于获取集群状态和提交状态更新任务
    private final ClusterService clusterService;

    // 负责分片路由策略的组件
    private final ShardsRoutingStrategy shardsRoutingStrategy;

    // 索引服务，用于操作索引级别的元数据
    private final IndicesService indicesService;

    // 节点索引创建动作，用于处理索引在节点上创建的事件
    private final NodeIndexCreatedAction nodeIndexCreatedAction;

    // 节点索引删除动作，用于处理索引在节点上删除的事件
    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    @Inject public MetaDataService(Settings settings, ClusterService clusterService, IndicesService indicesService, ShardsRoutingStrategy shardsRoutingStrategy,
                                   NodeIndexCreatedAction nodeIndexCreatedAction, NodeIndexDeletedAction nodeIndexDeletedAction) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardsRoutingStrategy = shardsRoutingStrategy;
        this.nodeIndexCreatedAction = nodeIndexCreatedAction;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
    }
    // 同步方法，用于创建一个新索引
    public synchronized boolean createIndex(final String index, final Settings indexSettings, TimeValue timeout) throws IndexAlreadyExistsException {
        // 检查集群状态，如果索引已存在，则抛出异常
        if (clusterService.state().routingTable().hasIndex(index)) {
            throw new IndexAlreadyExistsException(new Index(index));
        }
        // 执行索引名称的验证规则
        if (index.contains(" ")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain whitespace");
        }
        if (index.contains(",")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain ','");
        }
        if (index.contains("#")) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain '#'");
        }
        if (index.charAt(0) == '_') {
            throw new InvalidIndexNameException(new Index(index), index, "must not start with '_'");
        }
        if (!index.toLowerCase().equals(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must be lowercase");
        }
        if (!Strings.validFileName(index)) {
            throw new InvalidIndexNameException(new Index(index), index, "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }

        // 创建一个计数器，用于等待所有节点响应索引创建事件
        final CountDownLatch latch = new CountDownLatch(clusterService.state().nodes().size());
        // 创建节点索引创建事件的监听器
        NodeIndexCreatedAction.Listener nodeCreatedListener = new NodeIndexCreatedAction.Listener() {
            @Override public void onNodeIndexCreated(String mIndex, String nodeId) {
                if (index.equals(mIndex)) {
                    latch.countDown(); // 索引创建时递减计数器
                }
            }
        };
        // 注册监听器
        nodeIndexCreatedAction.add(nodeCreatedListener);
        // 提交集群状态更新任务，用于创建索引
        clusterService.submitStateUpdateTask("create-index [" + index + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                // 复制现有路由表并添加新索引的路由信息
                RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                    routingTableBuilder.add(indexRoutingTable);
                }
                // 构建新索引的设置，包括默认值
                ImmutableSettings.Builder indexSettingsBuilder = new ImmutableSettings.Builder().putAll(indexSettings);
                if (indexSettings.get(SETTING_NUMBER_OF_SHARDS) == null) {
                    indexSettingsBuilder.putInt(SETTING_NUMBER_OF_SHARDS, settings.getAsInt(SETTING_NUMBER_OF_SHARDS, 5));
                }
                if (indexSettings.get(SETTING_NUMBER_OF_REPLICAS) == null) {
                    indexSettingsBuilder.putInt(SETTING_NUMBER_OF_REPLICAS, settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, 1));
                }
                Settings actualIndexSettings = indexSettingsBuilder.build();

                // 创建新索引的元数据
                IndexMetaData indexMetaData = newIndexMetaDataBuilder(index).settings(actualIndexSettings).build();
                MetaData newMetaData = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                        .put(indexMetaData)
                        .build();

                // 初始化新索引的路由表
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index)
                        .initializeEmpty(newMetaData.index(index));
                routingTableBuilder.add(indexRoutingBuilder);

                // 记录创建索引的日志
                logger.info("Creating Index [{}], shards [{}]/[{}]", new Object[]{index, indexMetaData.numberOfShards(), indexMetaData.numberOfReplicas()});
                // 重新路由并返回新的集群状态
                RoutingTable newRoutingTable = shardsRoutingStrategy.reroute(newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).metaData(newMetaData).build();
            }
        });

        // 等待所有节点响应或超时
        try {
            return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        } finally {
            // 移除监听器
            nodeIndexCreatedAction.remove(nodeCreatedListener);
        }
    }
    // 同步方法，用于删除一个索引
    public synchronized boolean deleteIndex(final String index, TimeValue timeout) throws IndexMissingException {
        // 获取集群的路由表
        RoutingTable routingTable = clusterService.state().routingTable();
        // 检查索引是否存在，如果不存在则抛出IndexMissingException异常
        if (!routingTable.hasIndex(index)) {
            throw new IndexMissingException(new Index(index));
        }

        // 记录删除索引的日志
        logger.info("Deleting index [{}]", index);

        // 创建一个计数器，用于等待所有节点响应索引删除事件
        final CountDownLatch latch = new CountDownLatch(clusterService.state().nodes().size());
        // 创建节点索引删除事件的监听器
        NodeIndexDeletedAction.Listener listener = new NodeIndexDeletedAction.Listener() {
            @Override public void onNodeIndexDeleted(String fIndex, String nodeId) {
                // 如果事件中的索引与要删除的索引相匹配，则递减计数器
                if (fIndex.equals(index)) {
                    latch.countDown();
                }
            }
        };
        // 注册监听器
        nodeIndexDeletedAction.add(listener);
        // 提交集群状态更新任务，用于删除索引
        clusterService.submitStateUpdateTask("delete-index [" + index + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                // 创建新的路由表构建器
                RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                // 遍历当前状态的索引路由表
                for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                    // 如果索引路由表不是要删除的索引，则添加到新的路由表构建器中
                    if (!indexRoutingTable.index().equals(index)) {
                        routingTableBuilder.add(indexRoutingTable);
                    }
                }
                // 创建新的元数据构建器，移除指定的索引
                MetaData newMetaData = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                        .remove(index)
                        .build();

                // 重新路由并返回新的集群状态
                RoutingTable newRoutingTable = shardsRoutingStrategy.reroute(
                        newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).metaData(newMetaData).build();
            }
        });
        // 等待所有节点响应或超时
        try {
            return latch.await(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        } finally {
            // 移除监听器
            nodeIndexDeletedAction.remove(listener);
        }
    }

    // 方法，用于为一个或多个索引添加映射定义
    public void addMapping(final String[] indices, String mappingType, final String mappingSource) throws SearchException {
        // 获取当前集群状态
        ClusterState clusterState = clusterService.state();
        // 检查提供的索引是否都存在于集群状态中
        for (String index : indices) {
            IndexRoutingTable indexTable = clusterState.routingTable().indicesRouting().get(index);
            if (indexTable == null) {
                throw new IndexMissingException(new Index(index)); // 如果索引不存在，抛出异常
            }
        }

        // 尝试解析映射定义，并在所有索引的服务中进行验证
        DocumentMapper documentMapper = null;
        for (String index : indices) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                // 如果索引服务存在，解析映射定义
                documentMapper = indexService.mapperService().parse(mappingType, mappingSource);
            } else {
                throw new IndexMissingException(new Index(index)); // 如果索引服务不存在，抛出异常
            }
        }

        // 如果映射类型未提供，则使用解析得到的映射类型
        if (mappingType == null) {
            mappingType = documentMapper.type();
        } else if (!mappingType.equals(documentMapper.type())) {
            // 如果提供的映射类型与解析得到的不匹配，抛出异常
            throw new InvalidTypeNameException("Type name provided does not match type name within mapping definition");
        }
        // 检查映射类型名称是否以 '_' 开头，这是不允许的
        if (mappingType.charAt(0) == '_') {
            throw new InvalidTypeNameException("Document mapping type name can't start with '_'");
        }

        // 记录添加映射的日志
        logger.info("Indices [" + Arrays.toString(indices) + "]: Creating mapping [" + mappingType + "] with source [" + mappingSource + "]");

        // 提交一个集群状态更新任务，用于添加映射
        final String mappingTypeP = mappingType;
        clusterService.submitStateUpdateTask("create-mapping [" + mappingTypeP + "]", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder builder = newMetaDataBuilder().metaData(currentState.metaData());
                // 对于提供的每个索引，添加映射
                for (String indexName : indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(indexName)); // 如果索引元数据不存在，抛出异常
                    }
                    // 添加映射到索引元数据
                    builder.put(newIndexMetaDataBuilder(indexMetaData).addMapping(mappingTypeP, mappingSource));
                }
                // 返回更新后的集群状态
                return newClusterStateBuilder().state(currentState).metaData(builder).build();
            }
        });
    }

}
