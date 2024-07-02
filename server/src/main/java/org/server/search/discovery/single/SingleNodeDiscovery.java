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

package org.server.search.discovery.single;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.cluster.ClusterChangedEvent;
import org.server.search.cluster.ClusterName;
import org.server.search.cluster.ClusterState;
import org.server.search.discovery.Discovery;
import org.server.search.discovery.MasterService;
import org.server.search.transport.TransportService;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.settings.Settings;


import java.util.Objects;
/**
 * A discovery implementation where the only member of the cluster is the local node.
 */
public class SingleNodeDiscovery implements Discovery, LifecycleComponent {
    private static final Logger logger = LogManager.getLogger(SingleNodeDiscovery.class);

    private final ClusterName clusterName;
    protected final TransportService transportService;
    private final ClusterApplier clusterApplier;
    private volatile ClusterState clusterState;

    public SingleNodeDiscovery(final Settings settings, final TransportService transportService,
                               final MasterService masterService, final ClusterApplier clusterApplier,
                               final GatewayMetaState gatewayMetaState) {
        super(Objects.requireNonNull(settings));
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.transportService = Objects.requireNonNull(transportService);
        masterService.setClusterStateSupplier(() -> clusterState);
        this.clusterApplier = clusterApplier;

        if (clusterApplier instanceof ClusterApplierService) {
            ((ClusterApplierService) clusterApplier).addLowPriorityApplier(gatewayMetaState);
        }
    }

    @Override
    public synchronized void publish(final ClusterChangedEvent event, ActionListener<Void> publishListener,
                                     final AckListener ackListener) {
        clusterState = event.state();
        ackListener.onCommit(TimeValue.ZERO);

        clusterApplier.onNewClusterState("apply-locally-on-node[" + event.source() + "]", () -> clusterState, new ClusterApplyListener() {
            @Override
            public void onSuccess(String source) {
                publishListener.onResponse(null);
                ackListener.onNodeAck(transportService.getLocalNode(), null);
            }

            @Override
            public void onFailure(String source, Exception e) {
                publishListener.onFailure(e);
                ackListener.onNodeAck(transportService.getLocalNode(), e);
                logger.warn(() -> new ParameterizedMessage("failed while applying cluster state locally [{}]", event.source()), e);
            }
        });
    }

    @Override
    public DiscoveryStats stats() {
        return new DiscoveryStats(null, null);
    }

    @Override
    public synchronized void startInitialJoin() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("can't start initial join when not started");
        }
        // apply a fresh cluster state just so that state recovery gets triggered by GatewayService
        // TODO: give discovery module control over GatewayService
        clusterState = ClusterState.builder(clusterState).build();
        clusterApplier.onNewClusterState("single-node-start-initial-join", () -> clusterState, (source, e) -> {});
    }

    @Override
    protected synchronized void doStart() {
        // set initial state
        DiscoveryNode localNode = transportService.getLocalNode();
        clusterState = createInitialState(localNode);
        clusterApplier.setInitialState(clusterState);
    }

    protected ClusterState createInitialState(DiscoveryNode localNode) {
        ClusterState.Builder builder = ClusterState.builder(clusterName);
        return builder.nodes(DiscoveryNodes.builder().add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(localNode.getId())
                .build())
            .blocks(ClusterBlocks.builder()
                .addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
            .build();
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public Object start() throws Exception {
        return null;
    }

    @Override
    public Object stop() throws SearchException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws SearchException, InterruptedException {

    }
}
