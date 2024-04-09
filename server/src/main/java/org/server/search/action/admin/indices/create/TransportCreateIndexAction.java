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

package org.server.search.action.admin.indices.create;

import com.google.inject.Inject;
import org.server.search.ElasticSearchException;
import org.server.search.action.TransportActions;
import org.server.search.action.support.master.TransportMasterNodeOperationAction;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.metadata.MetaDataService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportCreateIndexAction extends TransportMasterNodeOperationAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetaDataService metaDataService;

    @Inject public TransportCreateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                              ThreadPool threadPool, MetaDataService metaDataService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataService = metaDataService;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.CREATE;
    }

    @Override protected CreateIndexRequest newRequest() {
        return new CreateIndexRequest();
    }

    @Override protected CreateIndexResponse newResponse() {
        return new CreateIndexResponse();
    }

    @Override protected CreateIndexResponse masterOperation(CreateIndexRequest request) throws ElasticSearchException {
        metaDataService.createIndex(request.index(), request.settings(), request.timeout());
        return new CreateIndexResponse();
    }
}
