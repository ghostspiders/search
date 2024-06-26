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

import java.io.IOException;

/**
 * Triggered by a {@link StartJoinRequest}, instances of this class represent join votes,
 * and have a source and target node. The source node is the node that provides the vote,
 * and the target node is the node for which this vote is cast. A node will only cast
 * a single vote per term, and this for a unique target node. The vote also carries
 * information about the current state of the node that provided the vote, so that
 * the receiver of the vote can determine if it has a more up-to-date state than the
 * source node.
 */
public class Join {
    /**
     * 源节点，表示发送当前操作或请求的节点。
     * 在节点发现、集群状态更新等过程中，这个变量用于标识发起操作的节点。
     */
    private final DiscoveryNode sourceNode;

    /**
     * 目标节点，表示当前操作或请求的目标节点。
     * 在节点加入、数据分片分配等场景中，这个变量用于标识操作的目标节点。
     */
    private final DiscoveryNode targetNode;

    /**
     * 当前任期，用于标识集群中的一个选举周期。
     * 在Raft算法或其他基于任期的共识算法中，每个任期都会有一个唯一的标识符。
     */
    private final long term;

    /**
     * 上一次接受的任期，表示节点最后一次成功加入集群并接受集群状态的任期。
     * 这个值用于在选举和状态同步过程中确保数据的一致性。
     */
    private final long lastAcceptedTerm;

    /**
     * 上一次接受的版本，表示节点最后一次成功加入集群并接受集群状态的版本号。
     * 这个值通常用于追踪集群状态的变更，确保状态更新的顺序性。
     */
    private final long lastAcceptedVersion;
    /**
     * 初始化一个新的Join实例，该实例表示节点加入集群的请求或操作。
     *
     * @param sourceNode 加入请求的发起节点，即源节点。
     * @param targetNode 加入请求的目标节点，通常是想要加入的集群中的主节点。
     * @param term 当前任期，用于在选举和集群状态管理中标识时间序列。
     * @param lastAcceptedTerm 节点最后一次接受的任期，用于确保集群状态的一致性。
     * @param lastAcceptedVersion 节点最后一次接受的集群状态版本，用于追踪状态变更。
     */
    public Join(DiscoveryNode sourceNode, DiscoveryNode targetNode, long term, long lastAcceptedTerm, long lastAcceptedVersion) {
        // 断言确保传入的任期和版本号不小于0
        assert term >= 0;
        assert lastAcceptedTerm >= 0;
        assert lastAcceptedVersion >= 0;

        // 初始化类的成员变量
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.term = term;
        this.lastAcceptedTerm = lastAcceptedTerm;
        this.lastAcceptedVersion = lastAcceptedVersion;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public boolean targetMatches(DiscoveryNode matchingNode) {
        return targetNode.getId().equals(matchingNode.getId());
    }

    public long getLastAcceptedVersion() {
        return lastAcceptedVersion;
    }

    public long getTerm() {
        return term;
    }

    public long getLastAcceptedTerm() {
        return lastAcceptedTerm;
    }

    @Override
    public String toString() {
        return "Join{" +
            "term=" + term +
            ", lastAcceptedTerm=" + lastAcceptedTerm +
            ", lastAcceptedVersion=" + lastAcceptedVersion +
            ", sourceNode=" + sourceNode +
            ", targetNode=" + targetNode +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Join join = (Join) o;

        if (sourceNode.equals(join.sourceNode) == false) return false;
        if (targetNode.equals(join.targetNode) == false) return false;
        if (lastAcceptedVersion != join.lastAcceptedVersion) return false;
        if (term != join.term) return false;
        return lastAcceptedTerm == join.lastAcceptedTerm;
    }

    @Override
    public int hashCode() {
        int result = (int) (lastAcceptedVersion ^ (lastAcceptedVersion >>> 32));
        result = 31 * result + sourceNode.hashCode();
        result = 31 * result + targetNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
        return result;
    }
}
