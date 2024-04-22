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

package org.server.search.client;

import org.server.search.action.ActionFuture;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.cluster.node.info.NodesInfoRequest;
import org.server.search.action.admin.cluster.node.info.NodesInfoResponse;
import org.server.search.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.server.search.action.admin.cluster.ping.broadcast.BroadcastPingResponse;
import org.server.search.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.server.search.action.admin.cluster.ping.replication.ReplicationPingResponse;
import org.server.search.action.admin.cluster.ping.single.SinglePingRequest;
import org.server.search.action.admin.cluster.ping.single.SinglePingResponse;
import org.server.search.action.admin.cluster.state.ClusterStateRequest;
import org.server.search.action.admin.cluster.state.ClusterStateResponse;

/**
 * 
 */
public interface ClusterAdminClient {

    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    void execState(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    ActionFuture<SinglePingResponse> ping(SinglePingRequest request);

    ActionFuture<SinglePingResponse> ping(SinglePingRequest request, ActionListener<SinglePingResponse> listener);

    void execPing(SinglePingRequest request, ActionListener<SinglePingResponse> listener);

    ActionFuture<BroadcastPingResponse> ping(BroadcastPingRequest request);

    ActionFuture<BroadcastPingResponse> ping(BroadcastPingRequest request, ActionListener<BroadcastPingResponse> listener);

    void execPing(BroadcastPingRequest request, ActionListener<BroadcastPingResponse> listener);

    ActionFuture<ReplicationPingResponse> ping(ReplicationPingRequest request);

    ActionFuture<ReplicationPingResponse> ping(ReplicationPingRequest request, ActionListener<ReplicationPingResponse> listener);

    void execPing(ReplicationPingRequest request, ActionListener<ReplicationPingResponse> listener);

    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    void execNodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);
}
