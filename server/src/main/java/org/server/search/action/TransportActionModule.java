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

package org.server.search.action;

import com.google.inject.AbstractModule;
import org.server.search.action.admin.cluster.node.info.TransportNodesInfo;
import org.server.search.action.admin.cluster.ping.broadcast.TransportBroadcastPingAction;
import org.server.search.action.admin.cluster.ping.replication.TransportIndexReplicationPingAction;
import org.server.search.action.admin.cluster.ping.replication.TransportReplicationPingAction;
import org.server.search.action.admin.cluster.ping.replication.TransportShardReplicationPingAction;
import org.server.search.action.admin.cluster.ping.single.TransportSinglePingAction;
import org.server.search.action.admin.cluster.state.TransportClusterStateAction;
import org.server.search.action.admin.indices.create.TransportCreateIndexAction;
import org.server.search.action.admin.indices.delete.TransportDeleteIndexAction;
import org.server.search.action.admin.indices.flush.TransportFlushAction;
import org.server.search.action.admin.indices.flush.TransportIndexFlushAction;
import org.server.search.action.admin.indices.flush.TransportShardFlushAction;
import org.server.search.action.admin.indices.gateway.snapshot.TransportGatewaySnapshotAction;
import org.server.search.action.admin.indices.gateway.snapshot.TransportIndexGatewaySnapshotAction;
import org.server.search.action.admin.indices.gateway.snapshot.TransportShardGatewaySnapshotAction;
import org.server.search.action.admin.indices.mapping.create.TransportCreateMappingAction;
import org.server.search.action.admin.indices.refresh.TransportIndexRefreshAction;
import org.server.search.action.admin.indices.refresh.TransportRefreshAction;
import org.server.search.action.admin.indices.refresh.TransportShardRefreshAction;
import org.server.search.action.admin.indices.status.TransportIndicesStatusAction;
import org.server.search.action.count.TransportCountAction;
import org.server.search.action.delete.TransportDeleteAction;
import org.server.search.action.deletebyquery.TransportDeleteByQueryAction;
import org.server.search.action.deletebyquery.TransportIndexDeleteByQueryAction;
import org.server.search.action.deletebyquery.TransportShardDeleteByQueryAction;
import org.server.search.action.get.TransportGetAction;
import org.server.search.action.index.TransportIndexAction;
import org.server.search.action.search.TransportSearchAction;
import org.server.search.action.search.TransportSearchScrollAction;
import org.server.search.action.search.type.*;

/**
 * 
 */
public class TransportActionModule extends AbstractModule {

    @Override protected void configure() {

        bind(TransportNodesInfo.class).asEagerSingleton();
        bind(TransportClusterStateAction.class).asEagerSingleton();

        bind(TransportSinglePingAction.class).asEagerSingleton();
        bind(TransportBroadcastPingAction.class).asEagerSingleton();
        bind(TransportShardReplicationPingAction.class).asEagerSingleton();
        bind(TransportIndexReplicationPingAction.class).asEagerSingleton();
        bind(TransportReplicationPingAction.class).asEagerSingleton();

        bind(TransportIndicesStatusAction.class).asEagerSingleton();
        bind(TransportCreateIndexAction.class).asEagerSingleton();
        bind(TransportCreateMappingAction.class).asEagerSingleton();
        bind(TransportDeleteIndexAction.class).asEagerSingleton();

        bind(TransportShardGatewaySnapshotAction.class).asEagerSingleton();
        bind(TransportIndexGatewaySnapshotAction.class).asEagerSingleton();
        bind(TransportGatewaySnapshotAction.class).asEagerSingleton();

        bind(TransportShardRefreshAction.class).asEagerSingleton();
        bind(TransportIndexRefreshAction.class).asEagerSingleton();
        bind(TransportRefreshAction.class).asEagerSingleton();

        bind(TransportShardFlushAction.class).asEagerSingleton();
        bind(TransportIndexFlushAction.class).asEagerSingleton();
        bind(TransportFlushAction.class).asEagerSingleton();

        bind(TransportIndexAction.class).asEagerSingleton();

        bind(TransportGetAction.class).asEagerSingleton();

        bind(TransportDeleteAction.class).asEagerSingleton();

        bind(TransportShardDeleteByQueryAction.class).asEagerSingleton();
        bind(TransportIndexDeleteByQueryAction.class).asEagerSingleton();
        bind(TransportDeleteByQueryAction.class).asEagerSingleton();

        bind(TransportCountAction.class).asEagerSingleton();

        bind(TransportSearchCache.class).asEagerSingleton();
        bind(TransportSearchDfsQueryThenFetchAction.class).asEagerSingleton();
        bind(TransportSearchQueryThenFetchAction.class).asEagerSingleton();
        bind(TransportSearchDfsQueryAndFetchAction.class).asEagerSingleton();
        bind(TransportSearchQueryAndFetchAction.class).asEagerSingleton();
        bind(TransportSearchAction.class).asEagerSingleton();

        bind(TransportSearchScrollQueryThenFetchAction.class).asEagerSingleton();
        bind(TransportSearchScrollAction.class).asEagerSingleton();
    }
}
