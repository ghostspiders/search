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
import org.server.search.client.AdminClient;
import org.server.search.client.ClusterAdminClient;
import org.server.search.client.IndicesAdminClient;
import org.server.search.client.transport.TransportClientNodesService;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.Settings;

/**
 * 
 */
public class InternalTransportAdminClient extends AbstractComponent implements AdminClient {

    private final TransportClientNodesService nodesService;

    private final InternalTransportIndicesAdminClient indicesAdminClient;

    private final InternalTransportClusterAdminClient clusterAdminClient;

    @Inject public InternalTransportAdminClient(Settings settings, TransportClientNodesService nodesService,
                                                InternalTransportIndicesAdminClient indicesAdminClient, InternalTransportClusterAdminClient clusterAdminClient) {
        super(settings);
        this.nodesService = nodesService;
        this.indicesAdminClient = indicesAdminClient;
        this.clusterAdminClient = clusterAdminClient;
    }

    @Override public IndicesAdminClient indices() {
        return indicesAdminClient;
    }

    @Override public ClusterAdminClient cluster() {
        return clusterAdminClient;
    }
}
