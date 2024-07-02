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

package org.server.search.discovery.jgroups;

import com.google.inject.Inject;
import io.netty.channel.ChannelException;
import org.server.search.SearchException;
import org.server.search.SearchIllegalStateException;
import org.server.search.cluster.ClusterName;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.ProcessedClusterStateUpdateTask;
import org.server.search.cluster.node.Node;
import org.server.search.discovery.Discovery;
import org.server.search.discovery.DiscoveryException;
import org.server.search.discovery.InitialStateDiscoveryListener;
import org.server.search.env.Environment;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.io.HostResolver;
import org.server.search.util.settings.Settings;
import org.jgroups.*;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.*;
import static org.server.search.cluster.ClusterState.*;
import static org.server.search.cluster.node.Nodes.*;

/**
 * A simplified discovery implementation based on JGroups that only works in client mode.
 *
 * 
 */
public class JgroupsClientDiscovery extends AbstractComponent implements Discovery, Receiver {

    private final Lifecycle lifecycle = new Lifecycle();

    private final ClusterName clusterName;

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final JChannel channel;

    private volatile ScheduledFuture reconnectFuture;

    private final AtomicBoolean initialStateSent = new AtomicBoolean();

    private final CopyOnWriteArrayList<InitialStateDiscoveryListener> initialStateListeners = new CopyOnWriteArrayList<InitialStateDiscoveryListener>();

    private final Node localNode = new Node("#client#", null); // dummy local node

    @Inject public JgroupsClientDiscovery(Settings settings, Environment environment, ClusterName clusterName, ClusterService clusterService, ThreadPool threadPool) throws Exception {
        super(settings);
        this.clusterName = clusterName;
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        String config = componentSettings.get("config", "udp");
        String actualConfig = config;
        if (!config.endsWith(".xml")) {
            actualConfig = "jgroups/" + config + ".xml";
        }
        URL configUrl = environment.resolveConfig(actualConfig);
        logger.debug("Using configuration [{}]", configUrl);

        Map<String, String> sysPropsSet = newHashMap();
        try {
            // prepare system properties to configure jgroups based on the settings
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                if (entry.getKey().startsWith("discovery.jgroups")) {
                    String jgroupsKey = entry.getKey().substring("discovery.".length());
                    if (System.getProperty(jgroupsKey) == null) {
                        sysPropsSet.put(jgroupsKey, entry.getValue());
                        System.setProperty(jgroupsKey, entry.getValue());
                    }
                }
            }

            if (System.getProperty("jgroups.bind_addr") == null) {
                // automatically set the bind address based on Search default bindings...
                try {
                    InetAddress bindAddress = HostResolver.resultBindHostAddress(null, settings, HostResolver.LOCAL_IP);
                    if ((bindAddress instanceof Inet4Address && HostResolver.isIPv4()) || (bindAddress instanceof Inet6Address && !HostResolver.isIPv4())) {
                        sysPropsSet.put("jgroups.bind_addr", bindAddress.getHostAddress());
                        System.setProperty("jgroups.bind_addr", bindAddress.getHostAddress());
                    }
                } catch (IOException e) {
                    // ignore this
                }
            }

            channel = new JChannel(configUrl);
        } catch (ChannelException e) {
            throw new DiscoveryException("Failed to create jgroups channel with config [" + configUrl + "]", e);
        } finally {
            for (String keyToRemove : sysPropsSet.keySet()) {
                System.getProperties().remove(keyToRemove);
            }
        }
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override
    public Discovery start() throws Exception {
        // 尝试将生命周期状态移动到已启动。如果已经是启动状态，则直接返回当前对象。
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        // 设置通道的接收器为当前实例，以便能够接收来自通道的消息。
        channel.setReceiver(this);

        try {
            // 尝试连接到集群。这里 clusterName.value() 应该是集群的名称或地址。
            channel.connect(clusterName.value());
        } catch (ChannelException e) {
            // 如果连接失败，抛出一个 DiscoveryException 异常，提供错误信息和原始异常。
            throw new DiscoveryException("Failed to connect to cluster [" + clusterName.value() + "]", e);
        }

        // 根据需要连接到主节点，这可能是为了获取集群状态或同步信息。
        connectTillMasterIfNeeded();

        // 如果需要，发送初始状态事件。这可能用于通知其他节点当前节点的状态或信息。
        sendInitialStateEventIfNeeded();

        // 方法执行成功，返回当前 Discovery 对象。
        return this;
    }

    @Override public Discovery stop() throws SearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        if (reconnectFuture != null) {
            reconnectFuture.cancel(true);
            reconnectFuture = null;
        }
        if (channel.isConnected()) {
            channel.disconnect();
        }
        return this;
    }

    @Override public void close() throws SearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        if (channel.isOpen()) {
            channel.close();
        }
    }

    @Override public void addListener(InitialStateDiscoveryListener listener) {
        initialStateListeners.add(listener);
    }

    @Override public void removeListener(InitialStateDiscoveryListener listener) {
        initialStateListeners.remove(listener);
    }

    @Override public void receive(Message msg) {
        if (msg.getSrc().equals(channel.getAddress())) {
            return; // my own message, ignore.
        }
        if (msg.getSrc().equals(channel.getView().getCreator())) {
            try {
                byte[] buffer = msg.getBuffer();
                final ClusterState origClusterState = ClusterState.Builder.fromBytes(buffer, settings, localNode);
                // remove the dummy local node
                final ClusterState clusterState = newClusterStateBuilder().state(origClusterState)
                        .nodes(newNodesBuilder().putAll(origClusterState.nodes()).remove(localNode.id())).build();
                System.err.println("Nodes: " + clusterState.nodes().prettyPrint());
                clusterService.submitStateUpdateTask("jgroups-disco-receive(from master)", new ProcessedClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        return clusterState;
                    }

                    @Override public void clusterStateProcessed(ClusterState clusterState) {
                        sendInitialStateEventIfNeeded();
                    }
                });
            } catch (Exception e) {
                logger.error("Received corrupted cluster state.", e);
            }
        }
    }

    @Override public void viewAccepted(View newView) {
        // we became master, reconnect
        if (channel.getAddress().equals(newView.getCreator())) {
            try {
                channel.disconnect();
            } catch (Exception e) {
                // ignore
            }
            if (!lifecycle.started()) {
                return;
            }
            connectTillMasterIfNeeded();
        }
    }

    private void sendInitialStateEventIfNeeded() {
        if (initialStateSent.compareAndSet(false, true)) {
            for (InitialStateDiscoveryListener listener : initialStateListeners) {
                listener.initialStateProcessed();
            }
        }
    }

    @Override public String nodeDescription() {
        return "clientNode";
    }

    @Override public void publish(ClusterState clusterState) {
        throw new SearchIllegalStateException("When in client mode, cluster state should not be published");
    }

    @Override public boolean firstMaster() {
        return false;
    }

    public byte[] getState() {
        return new byte[0];
    }

    public void setState(byte[] state) {
    }

    @Override public void suspect(Address suspectedMember) {
    }

    @Override public void block() {
        logger.warn("Blocked...");
    }

    private void connectTillMasterIfNeeded() {
        Runnable command = new Runnable() {
            @Override public void run() {
                try {
                    channel.connect(clusterName.value());
                    if (isMaster()) {
                        logger.debug("Act as master, reconnecting...");
                        channel.disconnect();
                        reconnectFuture = threadPool.schedule(this, 3, TimeUnit.SECONDS);
                    } else {
                        logger.debug("Reconnected not as master");
                        reconnectFuture = null;
                    }
                } catch (Exception e) {
                    logger.warn("Failed to connect to cluster", e);
                }
            }
        };

        if (channel.isConnected()) {
            if (!isMaster()) {
                logger.debug("Connected not as master");
                return;
            }
            channel.disconnect();
        }
        reconnectFuture = threadPool.schedule(command, 3, TimeUnit.SECONDS);
    }

    private boolean isMaster() {
        return channel.getAddress().equals(channel.getView().getCreator());
    }
}
