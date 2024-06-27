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

package org.server.search.transport;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.node.DiscoveryNode;
import org.server.search.cluster.node.Node;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.concurrent.highscalelib.NonBlockingHashMapLong;
import org.server.search.util.io.Streamable;
import org.server.search.util.settings.Settings;
import org.server.search.util.transport.BoundTransportAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.server.search.util.concurrent.ConcurrentMaps.*;
import static org.server.search.util.settings.ImmutableSettings.Builder.*;

 
public class TransportService extends AbstractComponent implements LifecycleComponent<TransportService> {

    // 生命周期管理对象，用于跟踪组件的启动、停止和关闭状态
    private final Lifecycle lifecycle = new Lifecycle();

    // 传输层对象，负责节点间的数据传输和通信
    private final Transport transport;

    // 并发映射，存储服务器端请求处理器，根据请求的名称进行索引
    private final ConcurrentMap<String, TransportRequestHandler> serverHandlers = newConcurrentMap();

    // 非阻塞哈希映射，存储客户端响应处理器，使用长整型作为键（通常用于请求ID）
    private final NonBlockingHashMapLong<TransportResponseHandler> clientHandlers = new NonBlockingHashMapLong<TransportResponseHandler>();

    // 原子长整型，用于生成唯一的请求ID
    final AtomicLong requestIds = new AtomicLong();
    public TransportService(Transport transport) {
        this(EMPTY_SETTINGS, transport);
    }

    @Inject public TransportService(Settings settings, Transport transport) {
        super(settings);
        this.transport = transport;
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    public TransportService start() throws Exception {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        // register us as an adapter for the transport service
        transport.transportServiceAdapter(new TransportServiceAdapter() {
            @Override public TransportRequestHandler handler(String action) {
                return serverHandlers.get(action);
            }

            @Override public TransportResponseHandler remove(long requestId) {
                return clientHandlers.remove(requestId);
            }
        });
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        return this;
    }

    public TransportService stop() throws SearchException, InterruptedException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        transport.stop();
        return this;
    }

    public void close() throws InterruptedException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        transport.close();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public void nodesAdded(Iterable<Node> nodes) {
        try {
            transport.nodesAdded(nodes);
        } catch (Exception e) {
            logger.warn("Failed add nodes [" + nodes + "] to transport", e);
        }
    }

    public void nodesRemoved(Iterable<Node> nodes) {
        try {
            transport.nodesRemoved(nodes);
        } catch (Exception e) {
            logger.warn("Failed to remove nodes[" + nodes + "] from transport", e);
        }
    }
    /**
     * 提交一个请求到指定的节点，并返回一个TransportFuture对象，用于获取响应。
     * @param node 目标节点
     * @param action 要执行的操作名称
     * @param message 请求消息，需要实现Streamable接口
     * @param handler 响应处理器，用于处理响应或异常
     * @return 一个TransportFuture对象，表示异步操作的结果
     * @throws TransportException 传输过程中发生异常
     */
    public <T extends Streamable> TransportFuture<T> submitRequest(Node node, String action, Streamable message,
                                                                   TransportResponseHandler<T> handler) throws TransportException {
        // 创建一个简单的TransportFuture包装器，用于处理响应
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<T>(handler);
        // 发送请求
        sendRequest(node, action, message, futureHandler);
        // 返回TransportFuture对象
        return futureHandler;
    }

    /**
     * 发送一个请求到指定的节点。
     * @param node 目标节点
     * @param action 要执行的操作名称
     * @param message 请求消息，需要实现Streamable接口
     * @param handler 响应处理器，用于处理响应或异常
     * @throws TransportException 传输过程中发生异常
     */
    public <T extends Streamable> void sendRequest(Node node, String action, Streamable message,
                                                   TransportResponseHandler<T> handler) throws TransportException {
        try {
            // 生成一个新的请求ID
            final long requestId = newRequestId();
            // 将请求ID和响应处理器关联起来，以便将来匹配响应
            clientHandlers.put(requestId, handler);
            // 通过传输层发送请求
            transport.sendRequest(node, requestId, action, message, handler);
        } catch (IOException e) {
            // 如果请求序列化失败，抛出TransportException异常
            throw new TransportException("Can't serialize request", e);
        }
    }


    private long newRequestId() {
        return requestIds.getAndIncrement();
    }

    public void registerHandler(ActionTransportRequestHandler handler) {
        registerHandler(handler.action(), handler);
    }

    public void registerHandler(String action, TransportRequestHandler handler) {
        serverHandlers.put(action, handler);
    }

    public void removeHandler(String action) {
        serverHandlers.remove(action);
    }

    public List<String> getLocalAddresses() {
        List<String> local = new ArrayList<>();
        local.add("127.0.0.1");
        return local;
    }
    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {

    }
}