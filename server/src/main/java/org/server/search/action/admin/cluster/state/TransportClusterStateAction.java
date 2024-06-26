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

package org.server.search.action.admin.cluster.state;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.action.TransportActions;
import org.server.search.action.support.master.TransportMasterNodeOperationAction;
import org.server.search.cluster.ClusterService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;


public class TransportClusterStateAction extends TransportMasterNodeOperationAction<ClusterStateRequest, ClusterStateResponse> {

    @Inject public TransportClusterStateAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Cluster.STATE;
    }

    @Override protected ClusterStateRequest newRequest() {
        return new ClusterStateRequest();
    }

    @Override protected ClusterStateResponse newResponse() {
        return new ClusterStateResponse();
    }

    @Override protected ClusterStateResponse masterOperation(ClusterStateRequest request) throws SearchException {
        return new ClusterStateResponse(clusterService.state());
    }
}