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

package org.server.search.client.server;

import com.google.inject.Inject;
import org.server.search.action.ActionFuture;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.indices.create.CreateIndexRequest;
import org.server.search.action.admin.indices.create.CreateIndexResponse;
import org.server.search.action.admin.indices.create.TransportCreateIndexAction;
import org.server.search.action.admin.indices.delete.DeleteIndexRequest;
import org.server.search.action.admin.indices.delete.DeleteIndexResponse;
import org.server.search.action.admin.indices.delete.TransportDeleteIndexAction;
import org.server.search.action.admin.indices.flush.FlushRequest;
import org.server.search.action.admin.indices.flush.FlushResponse;
import org.server.search.action.admin.indices.flush.TransportFlushAction;
import org.server.search.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.server.search.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.server.search.action.admin.indices.gateway.snapshot.TransportGatewaySnapshotAction;
import org.server.search.action.admin.indices.mapping.create.CreateMappingRequest;
import org.server.search.action.admin.indices.mapping.create.CreateMappingResponse;
import org.server.search.action.admin.indices.mapping.create.TransportCreateMappingAction;
import org.server.search.action.admin.indices.refresh.RefreshRequest;
import org.server.search.action.admin.indices.refresh.RefreshResponse;
import org.server.search.action.admin.indices.refresh.TransportRefreshAction;
import org.server.search.action.admin.indices.status.IndicesStatusRequest;
import org.server.search.action.admin.indices.status.IndicesStatusResponse;
import org.server.search.action.admin.indices.status.TransportIndicesStatusAction;
import org.server.search.client.IndicesAdminClient;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.Settings;

 
public class ServerIndicesAdminClient extends AbstractComponent implements IndicesAdminClient {

    private final TransportIndicesStatusAction indicesStatusAction;

    private final TransportCreateIndexAction createIndexAction;

    private final TransportDeleteIndexAction deleteIndexAction;

    private final TransportRefreshAction refreshAction;

    private final TransportFlushAction flushAction;

    private final TransportCreateMappingAction createMappingAction;

    private final TransportGatewaySnapshotAction gatewaySnapshotAction;

    @Inject public ServerIndicesAdminClient(Settings settings, TransportIndicesStatusAction indicesStatusAction,
                                            TransportCreateIndexAction createIndexAction, TransportDeleteIndexAction deleteIndexAction,
                                            TransportRefreshAction refreshAction, TransportFlushAction flushAction,
                                            TransportCreateMappingAction createMappingAction, TransportGatewaySnapshotAction gatewaySnapshotAction) {
        super(settings);
        this.indicesStatusAction = indicesStatusAction;
        this.createIndexAction = createIndexAction;
        this.deleteIndexAction = deleteIndexAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.createMappingAction = createMappingAction;
        this.gatewaySnapshotAction = gatewaySnapshotAction;
    }
    // 返回一个ActionFuture，表示对IndicesStatusRequest的请求将异步处理
    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request) {
        return indicesStatusAction.submit(request); // 提交请求到indicesStatusAction
    }

    // 同样提交IndicesStatusRequest，但提供一个ActionListener来处理结果
    @Override public ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        return indicesStatusAction.submit(request, listener); // 提交请求和监听器到indicesStatusAction
    }

    // 直接执行IndicesStatusRequest，使用ActionListener来接收结果
    @Override public void execStatus(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener) {
        indicesStatusAction.execute(request, listener); // 直接执行操作并传递监听器
    }

    // 创建索引的异步方法，返回ActionFuture
    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request) {
        return createIndexAction.submit(request); // 提交请求到createIndexAction
    }

    // 创建索引的异步方法，包括ActionListener
    @Override public ActionFuture<CreateIndexResponse> create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        return createIndexAction.submit(request, listener); // 提交请求和监听器到createIndexAction
    }

    // 直接执行创建索引操作，使用ActionListener
    @Override public void execCreate(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        createIndexAction.execute(request, listener); // 直接执行创建索引操作并传递监听器
    }

    // 删除索引的异步方法，返回ActionFuture
    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request) {
        return deleteIndexAction.submit(request); // 提交请求到deleteIndexAction
    }

    // 删除索引的异步方法，包括ActionListener
    @Override public ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        return deleteIndexAction.submit(request, listener); // 提交请求和监听器到deleteIndexAction
    }

    // 直接执行删除索引操作，使用ActionListener
    @Override public void execDelete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        deleteIndexAction.execute(request, listener); // 直接执行删除索引操作并传递监听器
    }

    // 刷新索引的异步方法，返回ActionFuture
    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request) {
        return refreshAction.submit(request); // 提交请求到refreshAction
    }

    // 刷新索引的异步方法，包括ActionListener
    @Override public ActionFuture<RefreshResponse> refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        return refreshAction.submit(request, listener); // 提交请求和监听器到refreshAction
    }

    // 直接执行刷新索引操作，使用ActionListener
    @Override public void execRefresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        refreshAction.execute(request, listener); // 直接执行刷新索引操作并传递监听器
    }

    // 清空索引的异步方法，返回ActionFuture
    @Override public ActionFuture<FlushResponse> flush(FlushRequest request) {
        return flushAction.submit(request); // 提交请求到flushAction
    }

    // 清空索引的异步方法，包括ActionListener
    @Override public ActionFuture<FlushResponse> flush(FlushRequest request, ActionListener<FlushResponse> listener) {
        return flushAction.submit(request, listener); // 提交请求和监听器到flushAction
    }

    // 直接执行清空索引操作，使用ActionListener
    @Override public void execFlush(FlushRequest request, ActionListener<FlushResponse> listener) {
        flushAction.execute(request, listener); // 直接执行清空索引操作并传递监听器
    }

    // 创建映射的异步方法，返回ActionFuture
    @Override public ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request) {
        return createMappingAction.submit(request); // 提交请求到createMappingAction
    }

    // 创建映射的异步方法，包括ActionListener
    @Override public ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener) {
        // 注意：这里方法签名有误，应为createMappingAction.submit(request, listener)
        return createMapping(request, listener); // 应修改为createMappingAction.submit(request, listener)
    }

    // 直接执行创建映射操作，使用ActionListener
    @Override public void execCreateMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener) {
        createMappingAction.execute(request, listener); // 直接执行创建映射操作并传递监听器
    }

    // 执行网关快照的异步方法，返回ActionFuture
    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request) {
        return gatewaySnapshotAction.submit(request); // 提交请求到gatewaySnapshotAction
    }

    // 执行网关快照的异步方法，包括ActionListener
    @Override public ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        return gatewaySnapshotAction.submit(request, listener); // 提交请求和监听器到gatewaySnapshotAction
    }

    // 直接执行网关快照操作，使用ActionListener
    @Override public void execGatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener) {
        gatewaySnapshotAction.execute(request, listener); // 直接执行网关快照操作并传递监听器
    }
}
