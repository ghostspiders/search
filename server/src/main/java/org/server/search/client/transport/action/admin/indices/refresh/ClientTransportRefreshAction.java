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

package org.server.search.client.transport.action.admin.indices.refresh;

import com.google.inject.Inject;
import org.server.search.action.TransportActions;
import org.server.search.action.admin.indices.refresh.RefreshRequest;
import org.server.search.action.admin.indices.refresh.RefreshResponse;
import org.server.search.client.transport.action.support.BaseClientTransportAction;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

/**
 * 
 */
public class ClientTransportRefreshAction extends BaseClientTransportAction<RefreshRequest, RefreshResponse> {

    @Inject public ClientTransportRefreshAction(Settings settings, TransportService transportService) {
        super(settings, transportService, RefreshResponse.class);
    }

    @Override protected String action() {
        return TransportActions.Admin.Indices.REFRESH;
    }
}