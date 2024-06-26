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

package org.server.search.cluster.node;

import com.google.common.collect.HashBiMap;
import org.server.search.util.Booleans;
import org.server.search.util.transport.InetSocketTransportAddress;

import java.util.*;
import java.util.stream.StreamSupport;

/**
 * This class holds all {@link DiscoveryNode} in the cluster and provides convenience methods to
 * access, modify merge / diff discovery nodes.
 */
public class DiscoveryNodes implements Iterable<DiscoveryNode> {

    public static final DiscoveryNodes EMPTY_NODES = builder().build();

    private final HashBiMap<String, DiscoveryNode> nodes;
    private final HashBiMap<String, DiscoveryNode> dataNodes;
    private final HashBiMap<String, DiscoveryNode> masterNodes;
    private final HashBiMap<String, DiscoveryNode> ingestNodes;

    private final String masterNodeId;
    private final String localNodeId;

    private DiscoveryNodes(HashBiMap<String, DiscoveryNode> nodes, HashBiMap<String, DiscoveryNode> dataNodes,
                           HashBiMap<String, DiscoveryNode> masterNodes, HashBiMap<String, DiscoveryNode> ingestNodes,
                           String masterNodeId, String localNodeId) {
        this.nodes = nodes;
        this.dataNodes = dataNodes;
        this.masterNodes = masterNodes;
        this.ingestNodes = ingestNodes;
        this.masterNodeId = masterNodeId;
        this.localNodeId = localNodeId;

    }

    @Override
    public Iterator<DiscoveryNode> iterator() {
        return nodes.values().iterator();
    }

    /**
     * Returns {@code true} if the local node is the elected master node.
     */
    public boolean isLocalNodeElectedMaster() {
        if (localNodeId == null) {
            // we don't know yet the local node id, return false
            return false;
        }
        return localNodeId.equals(masterNodeId);
    }

    /**
     * Get the number of known nodes
     *
     * @return number of nodes
     */
    public int getSize() {
        return nodes.size();
    }

    /**
     * Get a {@link Map} of the discovered nodes arranged by their ids
     *
     * @return {@link Map} of the discovered nodes arranged by their ids
     */
    public HashBiMap<String, DiscoveryNode> getNodes() {
        return this.nodes;
    }

    /**
     * Get a {@link Map} of the discovered data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered data nodes arranged by their ids
     */
    public HashBiMap<String, DiscoveryNode> getDataNodes() {
        return this.dataNodes;
    }

    /**
     * Get a {@link Map} of the discovered master nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master nodes arranged by their ids
     */
    public HashBiMap<String, DiscoveryNode> getMasterNodes() {
        return this.masterNodes;
    }

    /**
     * @return All the ingest nodes arranged by their ids
     */
    public HashBiMap<String, DiscoveryNode> getIngestNodes() {
        return ingestNodes;
    }

    /**
     * Get a {@link Map} of the discovered master and data nodes arranged by their ids
     *
     * @return {@link Map} of the discovered master and data nodes arranged by their ids
     */
    public HashBiMap<String, DiscoveryNode> getMasterAndDataNodes() {
        HashBiMap<String, DiscoveryNode> nodes = HashBiMap.create();
        nodes.putAll(dataNodes);
        nodes.putAll(masterNodes);
        return nodes;
    }

    /**
     * Get a {@link Map} of the coordinating only nodes (nodes which are neither master, nor data, nor ingest nodes) arranged by their ids
     *
     * @return {@link Map} of the coordinating only nodes arranged by their ids
     */
    public HashBiMap<String, DiscoveryNode> getCoordinatingOnlyNodes() {
        HashBiMap<String, DiscoveryNode> nodes = HashBiMap.create();
        nodes.putAll(this.nodes);
        for (String key : masterNodes.keySet()) {
            nodes.remove(key);
        }
        for (String key : dataNodes.keySet()) {
            nodes.remove(key);
        }
        for (String key : ingestNodes.keySet()) {
            nodes.remove(key);
        }
        return nodes;
    }


    /**
     * Get a node by its id
     *
     * @param nodeId id of the wanted node
     * @return wanted node if it exists. Otherwise <code>null</code>
     */
    public DiscoveryNode get(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Determine if a given node id exists
     *
     * @param nodeId id of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    /**
     * Determine if a given node exists
     *
     * @param node of the node which existence should be verified
     * @return <code>true</code> if the node exists. Otherwise <code>false</code>
     */
    public boolean nodeExists(DiscoveryNode node) {
        DiscoveryNode existing = nodes.get(node.getId());
        return existing != null && existing.equals(node);
    }

    /**
     * Get the id of the master node
     *
     * @return id of the master
     */
    public String getMasterNodeId() {
        return this.masterNodeId;
    }

    /**
     * Get the id of the local node
     *
     * @return id of the local node
     */
    public String getLocalNodeId() {
        return this.localNodeId;
    }

    /**
     * Get the local node
     *
     * @return local node
     */
    public DiscoveryNode getLocalNode() {
        return nodes.get(localNodeId);
    }

    /**
     * Returns the master node, or {@code null} if there is no master node
     */
    public DiscoveryNode getMasterNode() {
        if (masterNodeId != null) {
            return nodes.get(masterNodeId);
        }
        return null;
    }

    public DiscoveryNode findByAddress(InetSocketTransportAddress address) {
        for (DiscoveryNode node : nodes.values()) {
            if (node.getAddress().equals(address)) {
                return node;
            }
        }
        return null;
    }
    /**
     * Resolve a node with a given id
     *
     * @param node id of the node to discover
     * @return discovered node matching the given id
     * @throws IllegalArgumentException if more than one node matches the request or no nodes have been resolved
     */
    public DiscoveryNode resolveNode(String node) {
        String[] resolvedNodeIds = resolveNodes(node);
        if (resolvedNodeIds.length > 1) {
            throw new IllegalArgumentException("resolved [" + node + "] into [" + resolvedNodeIds.length
                + "] nodes, where expected to be resolved to a single node");
        }
        if (resolvedNodeIds.length == 0) {
            throw new IllegalArgumentException("failed to resolve [" + node + "], no matching nodes");
        }
        return nodes.get(resolvedNodeIds[0]);
    }

    /**
     *将一组节点“描述”解析为具体的和现有的节点id。“描述”可以（按此顺序解析）：
     *-相关节点的“_local”或“_master”
     *-节点id
     *-将与节点名称匹配的通配符模式
     *-一个“attr:value”模式，其中attr可以是一个节点角色（主、数据、摄取等），在这种情况下，值可以是true或false，
     *或者通用节点属性名称，在这种情况下，值将被视为通配符并与节点属性值匹配。
     **/
    public String[] resolveNodes(String... nodes) {
        if (nodes == null || nodes.length == 0) {
            return StreamSupport.stream(this.spliterator(), false).map(DiscoveryNode::getId).toArray(String[]::new);
        } else {
            Set<String> resolvedNodesIds = new HashSet<>(nodes.length);
            for (String nodeId : nodes) {
                if (nodeId.equals("_local")) {
                    String localNodeId = getLocalNodeId();
                    if (localNodeId != null) {
                        resolvedNodesIds.add(localNodeId);
                    }
                } else if (nodeId.equals("_master")) {
                    String masterNodeId = getMasterNodeId();
                    if (masterNodeId != null) {
                        resolvedNodesIds.add(masterNodeId);
                    }
                } else if (nodeExists(nodeId)) {
                    resolvedNodesIds.add(nodeId);
                } else {
                    for (DiscoveryNode node : this) {
                        if ("_all".equals(nodeId)
                                || nodeId.equals(node.getName())
                                || nodeId.equals(node.getHostAddress())
                                || nodeId.equals(node.getHostName())) {
                            resolvedNodesIds.add(node.getId());
                        }
                    }
                    int index = nodeId.indexOf(':');
                    if (index != -1) {
                        String matchAttrName = nodeId.substring(0, index);
                        String matchAttrValue = nodeId.substring(index + 1);
                        if (DiscoveryNode.Role.DATA.getRoleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(dataNodes.keySet());
                            } else {
                                resolvedNodesIds.removeAll(dataNodes.keySet());
                            }
                        } else if (DiscoveryNode.Role.MASTER.getRoleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(masterNodes.keySet());
                            } else {
                                resolvedNodesIds.removeAll(masterNodes.keySet());
                            }
                        } else if (DiscoveryNode.Role.INGEST.getRoleName().equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(ingestNodes.keySet());
                            } else {
                                resolvedNodesIds.removeAll(ingestNodes.keySet());
                            }
                        } else if (DiscoveryNode.COORDINATING_ONLY.equals(matchAttrName)) {
                            if (Booleans.parseBoolean(matchAttrValue, true)) {
                                resolvedNodesIds.addAll(getCoordinatingOnlyNodes().keySet());
                            } else {
                                resolvedNodesIds.removeAll(getCoordinatingOnlyNodes().keySet());
                            }
                        } else {
                            for (DiscoveryNode node : this) {
                                for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                                    String attrName = entry.getKey();
                                    String attrValue = entry.getValue();
                                    if (matchAttrName.equals(attrName) && matchAttrValue.equals( attrValue)) {
                                        resolvedNodesIds.add(node.getId());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return resolvedNodesIds.toArray(new String[0]);
        }
    }

    public DiscoveryNodes newNode(DiscoveryNode node) {
        return new Builder(this).add(node).build();
    }

    /**
     * Returns the changes comparing this nodes to the provided nodes.
     */
    public Delta delta(DiscoveryNodes other) {
        final List<DiscoveryNode> removed = new ArrayList<>();
        final List<DiscoveryNode> added = new ArrayList<>();
        for (DiscoveryNode node : other) {
            if (this.nodeExists(node) == false) {
                removed.add(node);
            }
        }
        for (DiscoveryNode node : this) {
            if (other.nodeExists(node) == false) {
                added.add(node);
            }
        }

        return new Delta(other.getMasterNode(), getMasterNode(), localNodeId, Collections.unmodifiableList(removed),
            Collections.unmodifiableList(added));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("nodes: \n");
        for (DiscoveryNode node : this) {
            sb.append("   ").append(node);
            if (node == getLocalNode()) {
                sb.append(", local");
            }
            if (node == getMasterNode()) {
                sb.append(", master");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public static class Delta {

        private final String localNodeId;
        private final DiscoveryNode previousMasterNode;
        private final DiscoveryNode newMasterNode;
        private final List<DiscoveryNode> removed;
        private final List<DiscoveryNode> added;

        private Delta(DiscoveryNode previousMasterNode, DiscoveryNode newMasterNode, String localNodeId,
                     List<DiscoveryNode> removed, List<DiscoveryNode> added) {
            this.previousMasterNode = previousMasterNode;
            this.newMasterNode = newMasterNode;
            this.localNodeId = localNodeId;
            this.removed = removed;
            this.added = added;
        }

        public boolean hasChanges() {
            return masterNodeChanged() || !removed.isEmpty() || !added.isEmpty();
        }

        public boolean masterNodeChanged() {
            return Objects.equals(newMasterNode, previousMasterNode) == false;
        }

        public DiscoveryNode previousMasterNode() {
            return previousMasterNode;
        }

        public DiscoveryNode newMasterNode() {
            return newMasterNode;
        }

        public boolean removed() {
            return !removed.isEmpty();
        }

        public List<DiscoveryNode> removedNodes() {
            return removed;
        }

        public boolean added() {
            return !added.isEmpty();
        }

        public List<DiscoveryNode> addedNodes() {
            return added;
        }

        public String shortSummary() {
            final StringBuilder summary = new StringBuilder();
            if (masterNodeChanged()) {
                summary.append("master node changed {previous [");
                if (previousMasterNode() != null) {
                    summary.append(previousMasterNode());
                }
                summary.append("], current [");
                if (newMasterNode() != null) {
                    summary.append(newMasterNode());
                }
                summary.append("]}");
            }
            if (removed()) {
                if (summary.length() > 0) {
                    summary.append(", ");
                }
                summary.append("removed {");
                for (DiscoveryNode node : removedNodes()) {
                    summary.append(node).append(',');
                }
                summary.append("}");
            }
            if (added()) {
                // don't print if there is one added, and it is us
                if (!(addedNodes().size() == 1 && addedNodes().get(0).getId().equals(localNodeId))) {
                    if (summary.length() > 0) {
                        summary.append(", ");
                    }
                    summary.append("added {");
                    for (DiscoveryNode node : addedNodes()) {
                        if (!node.getId().equals(localNodeId)) {
                            // don't print ourself
                            summary.append(node).append(',');
                        }
                    }
                    summary.append("}");
                }
            }
            return summary.toString();
        }
    }


    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(DiscoveryNodes nodes) {
        return new Builder(nodes);
    }

    public static class Builder {

        private  HashBiMap<String, DiscoveryNode> nodes = HashBiMap.create();
        private String masterNodeId;
        private String localNodeId;

        public Builder() {
        }

        public Builder(DiscoveryNodes nodes) {
            this.masterNodeId = nodes.getMasterNodeId();
            this.localNodeId = nodes.getLocalNodeId();
            this.nodes.putAll(nodes.getNodes());
        }

        /**
         * adds a disco node to the builder. Will throw an {@link IllegalArgumentException} if
         * the supplied node doesn't pass the pre-flight checks performed by {@link #validateAdd(DiscoveryNode)}
         */
        public Builder add(DiscoveryNode node) {
            final String preflight = validateAdd(node);
            if (preflight != null) {
                throw new IllegalArgumentException(preflight);
            }
            putUnsafe(node);
            return this;
        }

        /**
         * Get a node by its id
         *
         * @param nodeId id of the wanted node
         * @return wanted node if it exists. Otherwise <code>null</code>
         */
        public DiscoveryNode get(String nodeId) {
            return nodes.get(nodeId);
        }

        private void putUnsafe(DiscoveryNode node) {
            nodes.put(node.getId(), node);
        }

        public Builder remove(String nodeId) {
            nodes.remove(nodeId);
            return this;
        }

        public Builder remove(DiscoveryNode node) {
            if (node.equals(nodes.get(node.getId()))) {
                nodes.remove(node.getId());
            }
            return this;
        }


        public Builder masterNodeId(String masterNodeId) {
            this.masterNodeId = masterNodeId;
            return this;
        }

        public Builder localNodeId(String localNodeId) {
            this.localNodeId = localNodeId;
            return this;
        }

        /**
         * Checks that a node can be safely added to this node collection.
         *
         * @return null if all is OK or an error message explaining why a node can not be added.
         *
         * Note: if this method returns a non-null value, calling {@link #add(DiscoveryNode)} will fail with an
         * exception
         */
        private String validateAdd(DiscoveryNode node) {
            for (DiscoveryNode cursor : nodes.values()) {
                final DiscoveryNode existingNode = cursor;
                if (node.getAddress().equals(existingNode.getAddress()) &&
                    node.getId().equals(existingNode.getId()) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode + " with same address";
                }
                if (node.getId().equals(existingNode.getId()) &&
                    node.equals(existingNode) == false) {
                    return "can't add node " + node + ", found existing node " + existingNode
                        + " with the same id but is a different node instance";
                }
            }
            return null;
        }

        public DiscoveryNodes build() {
            HashBiMap<String, DiscoveryNode> dataNodesBuilder = HashBiMap.create();
            HashBiMap<String, DiscoveryNode> masterNodesBuilder = HashBiMap.create();
            HashBiMap<String, DiscoveryNode> ingestNodesBuilder = HashBiMap.create();
            for (Map.Entry<String, DiscoveryNode> nodeEntry : nodes.entrySet()) {
                if (nodeEntry.getValue().isDataNode()) {
                    dataNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                if (nodeEntry.getValue().isMasterNode()) {
                    masterNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
                if (nodeEntry.getValue().isIngestNode()) {
                    ingestNodesBuilder.put(nodeEntry.getKey(), nodeEntry.getValue());
                }
            }

            return new DiscoveryNodes(nodes, dataNodesBuilder, masterNodesBuilder, ingestNodesBuilder,
                    masterNodeId, localNodeId);
        }

        public boolean isLocalNodeElectedMaster() {
            return masterNodeId != null && masterNodeId.equals(localNodeId);
        }
    }
}
