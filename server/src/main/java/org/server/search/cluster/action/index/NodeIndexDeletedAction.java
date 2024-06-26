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

 
public class NodeIndexDeletedAction extends AbstractComponent {

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final List<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject public NodeIndexDeletedAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        transportService.registerHandler(NodeIndexDeletedTransportHandler.ACTION, new NodeIndexDeletedTransportHandler());
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    public void nodeIndexDeleted(final String index, final String nodeId) throws SearchException {
        Nodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    innerNodeIndexDeleted(index, nodeId);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    NodeIndexDeletedTransportHandler.ACTION, new NodeIndexDeletedMessage(index, nodeId), VoidTransportResponseHandler.INSTANCE);
        }
    }

    private void innerNodeIndexDeleted(String index, String nodeId) {
        for (Listener listener : listeners) {
            listener.onNodeIndexDeleted(index, nodeId);
        }
    }

    public static interface Listener {
        void onNodeIndexDeleted(String index, String nodeId);
    }

    private class NodeIndexDeletedTransportHandler extends BaseTransportRequestHandler<NodeIndexDeletedMessage> {

        static final String ACTION = "cluster/nodeIndexDeleted";

        @Override public NodeIndexDeletedMessage newInstance() {
            return new NodeIndexDeletedMessage();
        }

        @Override public void messageReceived(NodeIndexDeletedMessage message, TransportChannel channel) throws Exception {
            innerNodeIndexDeleted(message.index, message.nodeId);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }
    }

    private static class NodeIndexDeletedMessage implements Streamable {

        String index;

        String nodeId;

        private NodeIndexDeletedMessage() {
        }

        private NodeIndexDeletedMessage(String index, String nodeId) {
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