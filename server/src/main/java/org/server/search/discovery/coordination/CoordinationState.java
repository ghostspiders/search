/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.server.search.discovery.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.metadata.MetaData;
import org.server.search.util.settings.Settings;

import java.util.*;


/**
 * 集群状态协调算法的核心
 */
public class CoordinationState {

    // 日志记录器，用于记录CoordinationState类的日志信息
    private static final Logger logger = LogManager.getLogger(CoordinationState.class);
    // 持久化状态，存储需要在集群节点重启后保留的状态信息
    // 当前节点的引用
    private final DiscoveryNode localNode;
    private final PersistedState persistedState;
    public CoordinationState(Settings settings, DiscoveryNode localNode, PersistedState persistedState) {
        this.localNode = localNode;
        this.persistedState = persistedState;
    }
    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }
    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }
    /**
     * 用于{@link CoordinationState}的可插拔持久化层。
     */
    public interface PersistedState {

        /**
         * 返回当前任期。
         * @return 当前任期。
         */
        long getCurrentTerm();

        /**
         * 返回最后接受的集群状态。
         * @return 最后接受的集群状态。
         */
        ClusterState getLastAcceptedState();

        /**
         * 设置新的当前任期。
         * 成功调用此方法后，{@link #getCurrentTerm()}应该返回最后设置的任期。
         * {@link #getLastAcceptedState()}返回的值不应受调用此方法的影响。
         * @param currentTerm 要设置的新任期。
         */
        void setCurrentTerm(long currentTerm);

        /**
         * 设置新的最后接受的集群状态。
         * 成功调用此方法后，{@link #getLastAcceptedState()}应该返回最后设置的集群状态。
         * {@link #getCurrentTerm()}返回的值不应受调用此方法的影响。
         * @param clusterState 要设置的新集群状态。
         */
        void setLastAcceptedState(ClusterState clusterState);

        /**
         * 将最后接受的集群状态标记为已提交。
         * 成功调用此方法后，{@link #getLastAcceptedState()}应该返回最后设置的集群状态，
         * 最后提交的配置现在应与最后接受的配置相对应，如果设置了集群UUID，则将其标记为已提交。
         */
        default void markLastAcceptedStateAsCommitted() {
            final ClusterState lastAcceptedState = getLastAcceptedState(); // 获取最后接受的集群状态
            MetaData.Builder metaDataBuilder = null; // 元数据构建器

            // 如果最后接受的配置与最后提交的配置不相等，则更新协调元数据
            if (lastAcceptedState.getLastAcceptedConfiguration().equals(lastAcceptedState.getLastCommittedConfiguration()) == false) {
                final CoordinationMetaData coordinationMetaData = CoordinationMetaData.builder(lastAcceptedState.coordinationMetaData())
                    .lastCommittedConfiguration(lastAcceptedState.getLastAcceptedConfiguration())
                    .build();
                metaDataBuilder = MetaData.builder(lastAcceptedState.metaData());
                metaDataBuilder.coordinationMetaData(coordinationMetaData);
            }

            // 如果集群UUID已知但尚未标记为已提交，则将其标记为已提交
            if (lastAcceptedState.metaData().clusterUUID().equals(MetaData.UNKNOWN_CLUSTER_UUID) == false &&
                lastAcceptedState.metaData().clusterUUIDCommitted() == false) {
                if (metaDataBuilder == null) {
                    metaDataBuilder = MetaData.builder(lastAcceptedState.metaData());
                }
                metaDataBuilder.clusterUUIDCommitted(true);
                logger.info("cluster UUID set to [{}]", lastAcceptedState.metaData().clusterUUID());
            }

            // 如果更新了元数据，则使用新的元数据设置最后接受的集群状态
            if (metaDataBuilder != null) {
                setLastAcceptedState(ClusterState.builder(lastAcceptedState).metaData(metaDataBuilder).build());
            }
        }
    }

    public class VoteCollection {

        /**
         * 存储节点的映射，键为节点ID，值为节点对象。
         */
        private final Map<String, DiscoveryNode> nodes;

        /**
         * 为来自特定节点的投票添加到集合中。
         * @param sourceNode 投票的节点。
         * @return 如果集合中之前没有该节点的投票，则添加成功并返回true。
         */
        public boolean addVote(DiscoveryNode sourceNode) {
            return nodes.put(sourceNode.getId(), sourceNode) == null;
        }

        /**
         * 构造函数，初始化节点映射。
         */
        public VoteCollection() {
            nodes = new HashMap<>();
        }

        /**
         * 检查是否达到法定人数。
         * @param configuration 投票配置。
         * @return 如果根据投票配置达到法定人数，则返回true。
         */
        public boolean isQuorum(CoordinationMetaData.VotingConfiguration configuration) {
            return configuration.hasQuorum(nodes.keySet());
        }

        /**
         * 检查集合中是否包含特定节点的投票。
         * @param node 要检查的节点。
         * @return 如果集合包含该节点的投票，则返回true。
         */
        public boolean containsVoteFor(DiscoveryNode node) {
            return nodes.containsKey(node.getId());
        }

        /**
         * 检查投票集合是否为空。
         * @return 如果集合为空，则返回true。
         */
        public boolean isEmpty() {
            return nodes.isEmpty();
        }

        /**
         * 获取投票节点的集合。
         * @return 一个不可修改的节点集合视图。
         */
        public Collection<DiscoveryNode> nodes() {
            return Collections.unmodifiableCollection(nodes.values());
        }

        @Override
        public String toString() {
            // 返回节点ID列表的字符串表示。
            return "VoteCollection{" + String.join(",", nodes.keySet()) + "}";
        }

        @Override
        public boolean equals(Object o) {
            // 检查对象是否相等。
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VoteCollection that = (VoteCollection) o;

            return nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            // 返回此对象的哈希码，基于节点映射的哈希码。
            return nodes.hashCode();
        }
    }
}
