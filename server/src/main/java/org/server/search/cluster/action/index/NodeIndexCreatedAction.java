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

package org.server.search.cluster.action.index;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.node.Nodes;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.BaseTransportRequestHandler;
import org.server.search.transport.TransportChannel;
import org.server.search.transport.TransportService;
import org.server.search.transport.VoidTransportResponseHandler;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.io.Streamable;
import org.server.search.util.io.VoidStreamable;
import org.server.search.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class NodeIndexCreatedAction extends AbstractComponent {

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final List<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject public NodeIndexCreatedAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        transportService.registerHandler(NodeIndexCreatedTransportHandler.ACTION, new NodeIndexCreatedTransportHandler());
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    public void nodeIndexCreated(final String index, final String nodeId) throws SearchException {
        Nodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    innerNodeIndexCreated(index, nodeId);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    NodeIndexCreatedTransportHandler.ACTION, new NodeIndexCreatedMessage(index, nodeId), VoidTransportResponseHandler.INSTANCE);
        }
    }

    private void innerNodeIndexCreated(String index, String nodeId) {
        for (Listener listener : listeners) {
            listener.onNodeIndexCreated(index, nodeId);
        }
    }

    public static interface Listener {
        void onNodeIndexCreated(String index, String nodeId);
    }

    private class NodeIndexCreatedTransportHandler extends BaseTransportRequestHandler<NodeIndexCreatedMessage> {

        static final String ACTION = "cluster/nodeIndexCreated";

        @Override public NodeIndexCreatedMessage newInstance() {
            return new NodeIndexCreatedMessage();
        }

        @Override public void messageReceived(NodeIndexCreatedMessage message, TransportChannel channel) throws Exception {
            innerNodeIndexCreated(message.index, message.nodeId);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    private static class NodeIndexCreatedMessage implements Streamable {

        String index;

        String nodeId;

        private NodeIndexCreatedMessage() {
        }

        private NodeIndexCreatedMessage(String index, String nodeId) {
            this.index = index;
            this.nodeId = nodeId;
        }

        @Override public void writeTo(DataOutput out) throws IOException {
            out.writeUTF(index);
            out.writeUTF(nodeId);
        }

        @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            index = in.readUTF();
            nodeId = in.readUTF();
        }
    }
}
