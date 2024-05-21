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
import org.server.search.action.admin.indices.create.CreateIndexRequest;
import org.server.search.action.admin.indices.create.CreateIndexResponse;
import org.server.search.action.admin.indices.delete.DeleteIndexRequest;
import org.server.search.action.admin.indices.delete.DeleteIndexResponse;
import org.server.search.action.admin.indices.flush.FlushRequest;
import org.server.search.action.admin.indices.flush.FlushResponse;
import org.server.search.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.server.search.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.server.search.action.admin.indices.mapping.create.CreateMappingRequest;
import org.server.search.action.admin.indices.mapping.create.CreateMappingResponse;
import org.server.search.action.admin.indices.refresh.RefreshRequest;
import org.server.search.action.admin.indices.refresh.RefreshResponse;
import org.server.search.action.admin.indices.status.IndicesStatusRequest;
import org.server.search.action.admin.indices.status.IndicesStatusResponse;
import org.server.search.client.IndicesAdminClient;
import org.server.search.client.transport.TransportClientNodesService;
import org.server.search.client.transport.action.admin.indices.create.ClientTransportCreateIndexAction;
import org.server.search.client.transport.action.admin.indices.delete.ClientTransportDeleteIndexAction;
import org.server.search.client.transport.action.admin.indices.flush.ClientTransportFlushAction;
import org.server.search.client.transport.action.admin.indices.gateway.snapshot.ClientTransportGatewaySnapshotAction;
import org.server.search.client.transport.action.admin.indices.mapping.create.ClientTransportCreateMappingAction;
import org.server.search.client.transport.action.admin.indices.refresh.ClientTransportRefreshAction;
import org.server.search.client.transport.action.admin.indices.status.ClientTransportIndicesStatusAction;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.Settings;

 
public class InternalTransportIndicesAdminClient extends AbstractComponent implements IndicesAdminClient {

    private final TransportClientNodesService nodesService;

    private final ClientTransportIndicesStatusAction indicesStatusAction;

    private final ClientTransportCreateIndexAction createIndexAction;

    private final ClientTransportDeleteIndexAction deleteIndexAction;

    private final ClientTransportRefreshAction refreshAction;

    private final ClientTransportFlushAction flushAction;

    private final ClientTransportCreateMappingAction createMappingAction;

    private final ClientTransportGatewaySnapshotAction gatewaySnapshotAction;

    @Inject public InternalTransportIndicesAdminClient(Settings settings, TransportClientNodesService nodesService,
                                                       ClientTransportIndicesStatusAction indicesStatusAction,
                                                       ClientTransportCreateIndexAction createIndexAction, ClientTransportDeleteIndexAction deleteIndexAction,
                                                       ClientTransportRefreshAction refreshAction, ClientTransportFlushAction flushAction,
                                                       ClientTransportCreateMappingAction createMappingAction, ClientTransportGatewaySnapshotAction gatewaySnapshotAction) {
        super(settings);
        this.nodesService = nodesService;
        this.indicesStatusAction = indicesStatusAction;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.createMappingAction = createMappingAction;
        this.gatewaySnapshotAction = gatewaySnapshotAction;
    }

    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request) {
        return indicesStatusAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        return indicesStatusAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execStatus(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        indicesStatusAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request) {
        return createIndexAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        return createIndexAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execCreate(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        createIndexAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request) {
        return deleteIndexAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        return deleteIndexAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execDelete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        deleteIndexAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request) {
        return refreshAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        return refreshAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execRefresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        refreshAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<FlushResponse> flush(FlushRequest request) {
        return flushAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<FlushResponse> flush(FlushRequest request, ActionListener<FlushResponse> listener) {
        return flushAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execFlush(FlushRequest request, ActionListener<FlushResponse> listener) {
        flushAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request) {
        return createMappingAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener) {
        return createMappingAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execCreateMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener) {
        createMappingAction.execute(nodesService.randomNode(), request, listener);
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request) {
        return gatewaySnapshotAction.submit(nodesService.randomNode(), request);
    }

    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        return gatewaySnapshotAction.submit(nodesService.randomNode(), request, listener);
    }

    @Override public void execGatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        gatewaySnapshotAction.execute(nodesService.randomNode(), request, listener);
    }
}
