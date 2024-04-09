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

package org.server.search.jmx.action;

import com.google.inject.Inject;
import org.server.search.ElasticSearchException;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.node.Node;
import org.server.search.jmx.JmxService;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.FutureTransportResponseHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.io.StringStreamable;
import org.server.search.util.io.VoidStreamable;
import org.server.search.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class GetJmxServiceUrlAction extends AbstractComponent {

    private final JmxService jmxService;

    private final TransportService transportService;

    private final ClusterService clusterService;

    @Inject public GetJmxServiceUrlAction(Settings settings, JmxService jmxService,
                                          TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.jmxService = jmxService;
        this.transportService = transportService;
        this.clusterService = clusterService;

        transportService.registerHandler(GetJmxServiceUrlTransportHandler.ACTION, new GetJmxServiceUrlTransportHandler());
    }

    public String obtainPublishUrl(final Node node) throws ElasticSearchException {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            return jmxService.publishUrl();
        } else {
            return transportService.submitRequest(node, GetJmxServiceUrlTransportHandler.ACTION, VoidStreamable.INSTANCE, new FutureTransportResponseHandler<StringStreamable>() {
                @Override public StringStreamable newInstance() {
                    return new StringStreamable();
                }
            }).txGet().get();
        }
    }

    private class GetJmxServiceUrlTransportHandler extends BaseTransportRequestHandler<VoidStreamable> {

        static final String ACTION = "jmx/publishUrl";

        @Override public VoidStreamable newInstance() {
            return VoidStreamable.INSTANCE;
        }

        @Override public void messageReceived(VoidStreamable request, TransportChannel channel) throws Exception {
            channel.sendResponse(new StringStreamable(jmxService.publishUrl()));
        }
    }
}