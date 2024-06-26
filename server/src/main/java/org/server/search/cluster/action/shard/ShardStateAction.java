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

package org.server.search.cluster.action.shard;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.ClusterStateUpdateTask;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.ImmutableShardRouting;
import org.server.search.cluster.routing.IndexShardRoutingTable;
import org.server.search.cluster.routing.RoutingTable;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.cluster.routing.strategy.ShardsRoutingStrategy;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.transport.VoidTransportResponseHandler;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.io.VoidStreamable;
import org.server.search.util.settings.Settings;

import static com.google.common.collect.Lists.*;
import static org.server.search.cluster.ClusterState.*;


public class ShardStateAction extends AbstractComponent {

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final ShardsRoutingStrategy shardsRoutingStrategy;

    private final ThreadPool threadPool;

    @Inject public ShardStateAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                    ShardsRoutingStrategy shardsRoutingStrategy, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.shardsRoutingStrategy = shardsRoutingStrategy;
        this.threadPool = threadPool;

        transportService.registerHandler(ShardStartedTransportHandler.ACTION, new ShardStartedTransportHandler());
        transportService.registerHandler(ShardFailedTransportHandler.ACTION, new ShardFailedTransportHandler());
    }

    public void shardFailed(final ShardRouting shardRouting) throws SearchException {
        logger.warn("Sending failed shard for {}", shardRouting);
        Nodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    innerShardFailed(shardRouting);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    ShardFailedTransportHandler.ACTION, shardRouting, VoidTransportResponseHandler.INSTANCE);
        }
    }

    public void shardStarted(final ShardRouting shardRouting) throws SearchException {
        if (logger.isDebugEnabled()) {
            logger.debug("Sending shard started for {}", shardRouting);
        }
        Nodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    innerShardStarted(shardRouting);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    ShardStartedTransportHandler.ACTION, shardRouting, VoidTransportResponseHandler.INSTANCE);
        }
    }

    private void innerShardFailed(final ShardRouting shardRouting) {
        logger.warn("Received shard failed for {}", shardRouting);
        clusterService.submitStateUpdateTask("shard-failed (" + shardRouting + ")", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Applying failed shard {}", shardRouting);
                }
                RoutingTable prevRoutingTable = currentState.routingTable();
                RoutingTable newRoutingTable = shardsRoutingStrategy.applyFailedShards(currentState, newArrayList(shardRouting));
                if (prevRoutingTable == newRoutingTable) {
                    return currentState;
                }
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).build();
            }
        });
    }

    private void innerShardStarted(final ShardRouting shardRouting) {
        if (logger.isDebugEnabled()) {
            logger.debug("Received shard started for {}", shardRouting);
        }
        clusterService.submitStateUpdateTask("shard-started (" + shardRouting + ")", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                RoutingTable routingTable = currentState.routingTable();
                // find the one that maps to us, if its already started, no need to do anything...
                // the shard might already be started since the nodes that is starting the shards might get cluster events
                // with the shard still initializing, and it will try and start it again (until the verification comes)
                IndexShardRoutingTable indexShardRoutingTable = routingTable.index(shardRouting.index()).shard(shardRouting.id());
                for (ShardRouting entry : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId().equals(entry.currentNodeId())) {
                        // we found the same shard that exists on the same node id
                        if (entry.started()) {
                            // already started, do nothing here...
                            return currentState;
                        }
                    }
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Applying started shard {}", shardRouting);
                }
                RoutingTable newRoutingTable = shardsRoutingStrategy.applyStartedShards(currentState, newArrayList(shardRouting));
                if (routingTable == newRoutingTable) {
                    return currentState;
                }
                return newClusterStateBuilder().state(currentState).routingTable(newRoutingTable).build();
            }
        });
    }

    private class ShardFailedTransportHandler extends BaseTransportRequestHandler<ShardRouting> {

        static final String ACTION = "cluster/shardFailure";

        @Override public ShardRouting newInstance() {
            return new ImmutableShardRouting();
        }

        @Override public void messageReceived(ShardRouting request, TransportChannel channel) throws Exception {
            innerShardFailed(request);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    private class ShardStartedTransportHandler extends BaseTransportRequestHandler<ShardRouting> {

        static final String ACTION = "cluster/shardStarted";

        @Override public ShardRouting newInstance() {
            return new ImmutableShardRouting();
        }

        @Override public void messageReceived(ShardRouting request, TransportChannel channel) throws Exception {
            innerShardStarted(request);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }
}
