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
import org.server.search.cluster.node.DiscoveryNode;
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
    private boolean electionWon; // 表示当前节点是否赢得了选举
    private VoteCollection joinVotes; // 用于收集加入集群时其他节点的投票
    private boolean startedJoinSinceLastReboot; // 表示自上次重启以来是否已经开始加入集群
    public CoordinationState(Settings settings, DiscoveryNode localNode, PersistedState persistedState) {
        this.localNode = localNode;
        this.persistedState = persistedState;
        this.electionWon = false;
    }
    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }
    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }
    public boolean electionWon() {
        return electionWon;
    }
    /**
     * 可以随时安全调用，以将此实例移动到新的术语。
     *
     * @param startJoinRequest 指定请求加入的节点的开始加入请求。
     * @return 应该发送到加入目标节点的加入对象。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public Join handleStartJoin(StartJoinRequest startJoinRequest) throws Exception {
        // 如果提供的术语不大于当前术语，则忽略请求
        if (startJoinRequest.getTerm() <= getCurrentTerm()) {
            logger.debug("handleStartJoin: ignoring [{}] as term provided is not greater than current term [{}]",
                    startJoinRequest, getCurrentTerm());
            throw new Exception("incoming term " + startJoinRequest.getTerm() +
                    " not greater than current term " + getCurrentTerm());
        }

        // 记录日志，表示由于开始加入请求而离开当前术语
        logger.debug("handleStartJoin: leaving term [{}] due to {}", getCurrentTerm(), startJoinRequest);

        // 如果存在非空的加入投票，则记录日志并丢弃它们
        if (joinVotes.isEmpty() == false) {
            // ...
            logger.debug("handleStartJoin: discarding {}: {}", joinVotes, reason);
        }

        // 设置持久化状态的当前术语
        persistedState.setCurrentTerm(startJoinRequest.getTerm());
        // 断言当前术语等于开始加入请求的术语
        assert getCurrentTerm() == startJoinRequest.getTerm();
        // 重置最后发布版本
        lastPublishedVersion = 0;
        // 更新最后发布配置为最后接受的配置
        lastPublishedConfiguration = getLastAcceptedConfiguration();
        // 标记自上次重启以来已开始加入
        startedJoinSinceLastReboot = true;
        // 重置选举胜利状态
        electionWon = false;
        // 重置加入投票集合
        joinVotes = new VoteCollection();
        // 重置发布投票集合
        publishVotes = new VoteCollection();

        // 创建并返回一个新的加入对象
        return new Join(localNode, startJoinRequest.getSourceNode(), getCurrentTerm(), getLastAcceptedTerm(),
                getLastAcceptedVersionOrMetaDataVersion());
    }
    /**
     * 在收到加入请求（Join）时调用。
     *
     * @param join 收到的加入请求。
     * @return 如果此实例尚未为此术语自给定源节点收到加入投票，则返回true。
     * @throws CoordinationStateRejectedException 如果参数与此对象的当前状态不兼容。
     */
    public boolean handleJoin(Join join) throws Exception {
        // 断言处理的加入请求是针对本地节点的
        assert join.targetMatches(localNode) : "handling join " + join + " for the wrong node " + localNode;
        // 检查请求的任期是否与当前任期匹配
        if (join.getTerm() != getCurrentTerm()) {
            logger.debug("handleJoin: ignored join due to term mismatch (expected: [{}], actual: [{}])",
                    getCurrentTerm(), join.getTerm());
            throw new Exception(
                    "incoming term " + join.getTerm() + " does not match current term " + getCurrentTerm());
        }
        // 如果自上次重启以来任期尚未增加，则忽略加入请求
        if (startedJoinSinceLastReboot == false) {
            logger.debug("handleJoin: ignored join as term was not incremented yet after reboot");
            throw new CoordinationStateRejectedException("ignored join as term has not been incremented yet after reboot");
        }
        // 获取最后接受的任期
        final long lastAcceptedTerm = getLastAcceptedTerm();
        // 如果加入者的最后接受任期大于当前最后接受任期，则忽略加入请求
        if (join.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleJoin: ignored join as joiner has a better last accepted term (expected: <=[{}], actual: [{}])",
                    lastAcceptedTerm, join.getLastAcceptedTerm());
            throw new CoordinationStateRejectedException("incoming last accepted term " + join.getLastAcceptedTerm() +
                    " of join higher than current last accepted term " + lastAcceptedTerm);
        }
        // 如果加入者的最后接受任期与当前相同，但版本号大于最后接受的版本号，则忽略加入请求
        if (join.getLastAcceptedTerm() == lastAcceptedTerm && join.getLastAcceptedVersion() > getLastAcceptedVersionOrMetaDataVersion()) {
            logger.debug(
                    "handleJoin: ignored join as joiner has a better last accepted version (expected: <=[{}], actual: [{}]) in term {}",
                    getLastAcceptedVersionOrMetaDataVersion(), join.getLastAcceptedVersion(), lastAcceptedTerm);
            throw new CoordinationStateRejectedException("incoming last accepted version " + join.getLastAcceptedVersion() +
                    " of join higher than current last accepted version " + getLastAcceptedVersionOrMetaDataVersion()
                    + " in term " + lastAcceptedTerm);
        }
        // 如果尚未接收到初始配置，则拒绝加入请求
        if (getLastAcceptedConfiguration().isEmpty()) {
            // We do not check for an election won on setting the initial configuration, so it would be possible to end up in a state where
            // we have enough join votes to have won the election immediately on setting the initial configuration. It'd be quite
            // complicated to restore all the appropriate invariants when setting the initial configuration (it's not just electionWon)
            // so instead we just reject join votes received prior to receiving the initial configuration.
            logger.debug("handleJoin: rejecting join since this node has not received its initial configuration yet");
            throw new CoordinationStateRejectedException("rejecting join since this node has not received its initial configuration yet");
        }


        // 为加入请求的源节点添加投票
        boolean added = joinVotes.addVote(join.getSourceNode());
        // 记录选举胜利状态
        boolean prevElectionWon = electionWon;
        electionWon = isElectionQuorum(joinVotes);
        // 断言选举胜利状态不能从未胜利变为胜利
        assert !prevElectionWon || electionWon;
        // 记录加入请求处理的日志
        logger.debug("handleJoin: added join {} from [{}] for election, electionWon={} lastAcceptedTerm={} lastAcceptedVersion={}",
                join, join.getSourceNode(), electionWon, lastAcceptedTerm, getLastAcceptedVersion());

        // 如果选举胜利且之前未胜利，则更新最后发布版本
        if (electionWon && prevElectionWon == false) {
            logger.debug("handleJoin: election won in term [{}] with {}", getCurrentTerm(), joinVotes);
            lastPublishedVersion = getLastAcceptedVersion();
        }
        return added; // 返回是否成功添加了投票
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
