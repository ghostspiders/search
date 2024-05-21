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

package org.server.search.client.transport.support;

import com.google.inject.Inject;
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
import org.server.search.client.ClusterAdminClient;
import org.server.search.client.transport.TransportClientNodesService;
import org.server.search.client.transport.action.admin.cluster.node.info.ClientTransportNodesInfoAction;
import org.server.search.client.transport.action.admin.cluster.ping.broadcast.ClientTransportBroadcastPingAction;
import org.server.search.client.transport.action.admin.cluster.ping.replication.ClientTransportReplicationPingAction;
import org.server.search.client.transport.action.admin.cluster.ping.single.ClientTransportSinglePingAction;
import org.server.search.client.transport.action.admin.cluster.state.ClientTransportClusterStateAction;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.Settings;

 
public class InternalTransportClusterAdminClient extends AbstractComponent implements ClusterAdminClient {

    private final TransportClientNodesService nodesService;

    private final ClientTransportClusterStateAction clusterStateAction;

    private final ClientTransportSinglePingAction singlePingAction;

    private final ClientTransportReplicationPingAction replicationPingAction;

    private final ClientTransportBroadcastPingAction broadcastPingAction;

    private final ClientTransportNodesInfoAction nodesInfoAction;

    @Inject public InternalTransportClusterAdminClient(Settings settings, TransportClientNodesService nodesService,
                                                       ClientTransportClusterStateAction clusterStateAction,
                                                       ClientTransportSinglePingAction singlePingAction, ClientTransportReplicationPingAction replicationPingAction, ClientTransportBroadcastPingAction broadcastPingAction,
                                                       ClientTransportNodesInfoAction nodesInfoAction) {
        super(settings);
        this.nodesService = nodesService;
        this.clusterStateAction = clusterStateAction;
        this.nodesInfoAction = nodesInfoAction;
        this.singlePingAction = singlePingAction;
        this.replicationPingAction = replicationPingAction;
        this.broadcastPingAction = broadcastPingAction;
    }

    @Override public ActionFuture<ClusterStateResponse> state(ClusterStateRequest request) {
        return clusterStateAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<ClusterStateResponse> state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener) {
        return clusterStateAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execState(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener) {
        clusterStateAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<SinglePingResponse> ping(SinglePingRequest request) {
        return singlePingAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<SinglePingResponse> ping(SinglePingRequest request, ActionListener<SinglePingResponse> listener) {
        return singlePingAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execPing(SinglePingRequest request, ActionListener<SinglePingResponse> listener) {
        singlePingAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<BroadcastPingResponse> ping(BroadcastPingRequest request) {
        return broadcastPingAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<BroadcastPingResponse> ping(BroadcastPingRequest request, ActionListener<BroadcastPingResponse> listener) {
        return broadcastPingAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execPing(BroadcastPingRequest request, ActionListener<BroadcastPingResponse> listener) {
        broadcastPingAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<ReplicationPingResponse> ping(ReplicationPingRequest request) {
        return replicationPingAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<ReplicationPingResponse> ping(ReplicationPingRequest request, ActionListener<ReplicationPingResponse> listener) {
        return replicationPingAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execPing(ReplicationPingRequest request, ActionListener<ReplicationPingResponse> listener) {
        replicationPingAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request) {
        return nodesInfoAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
        return nodesInfoAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execNodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener) {
        nodesInfoAction.execute(nodesService.randomNode(), request, listener);
    }
}
