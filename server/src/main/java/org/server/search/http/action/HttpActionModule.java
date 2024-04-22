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

package org.server.search.http.action;

import com.google.inject.AbstractModule;
import org.server.search.http.action.admin.cluster.node.info.HttpNodesInfoAction;
import org.server.search.http.action.admin.cluster.ping.broadcast.HttpBroadcastPingAction;
import org.server.search.http.action.admin.cluster.ping.replication.HttpReplicationPingAction;
import org.server.search.http.action.admin.cluster.ping.single.HttpSinglePingAction;
import org.server.search.http.action.admin.cluster.state.HttpClusterStateAction;
import org.server.search.http.action.admin.indices.create.HttpCreateIndexAction;
import org.server.search.http.action.admin.indices.delete.HttpDeleteIndexAction;
import org.server.search.http.action.admin.indices.flush.HttpFlushAction;
import org.server.search.http.action.admin.indices.gateway.snapshot.HttpGatewaySnapshotAction;
import org.server.search.http.action.admin.indices.mapping.create.HttpCreateMappingAction;
import org.server.search.http.action.admin.indices.refresh.HttpRefreshAction;
import org.server.search.http.action.admin.indices.status.HttpIndicesStatusAction;
import org.server.search.http.action.count.HttpCountAction;
import org.server.search.http.action.delete.HttpDeleteAction;
import org.server.search.http.action.deletebyquery.HttpDeleteByQueryAction;
import org.server.search.http.action.get.HttpGetAction;
import org.server.search.http.action.index.HttpIndexAction;
import org.server.search.http.action.main.HttpMainAction;
import org.server.search.http.action.search.HttpSearchAction;

/**
 * 
 */
public class HttpActionModule extends AbstractModule {

    @Override protected void configure() {
        bind(HttpMainAction.class).asEagerSingleton();

        bind(HttpNodesInfoAction.class).asEagerSingleton();
        bind(HttpClusterStateAction.class).asEagerSingleton();

        bind(HttpSinglePingAction.class).asEagerSingleton();
        bind(HttpBroadcastPingAction.class).asEagerSingleton();
        bind(HttpReplicationPingAction.class).asEagerSingleton();

        bind(HttpIndicesStatusAction.class).asEagerSingleton();
        bind(HttpCreateIndexAction.class).asEagerSingleton();
        bind(HttpDeleteIndexAction.class).asEagerSingleton();

        bind(HttpCreateMappingAction.class).asEagerSingleton();

        bind(HttpGatewaySnapshotAction.class).asEagerSingleton();

        bind(HttpRefreshAction.class).asEagerSingleton();

        bind(HttpFlushAction.class).asEagerSingleton();

        bind(HttpIndexAction.class).asEagerSingleton();

        bind(HttpGetAction.class).asEagerSingleton();

        bind(HttpDeleteAction.class).asEagerSingleton();

        bind(HttpDeleteByQueryAction.class).asEagerSingleton();

        bind(HttpCountAction.class).asEagerSingleton();

        bind(HttpSearchAction.class).asEagerSingleton();
    }
}
