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

package org.server.search.client;

import org.server.search.action.admin.cluster.node.info.NodesInfoRequest;
import org.server.search.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.server.search.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.server.search.action.admin.cluster.ping.single.SinglePingRequest;
import org.server.search.action.admin.cluster.state.ClusterStateRequest;
import org.server.search.action.admin.indices.create.CreateIndexRequest;
import org.server.search.action.admin.indices.delete.DeleteIndexRequest;
import org.server.search.action.admin.indices.flush.FlushRequest;
import org.server.search.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.server.search.action.admin.indices.mapping.create.CreateMappingRequest;
import org.server.search.action.admin.indices.refresh.RefreshRequest;
import org.server.search.action.admin.indices.status.IndicesStatusRequest;
import org.server.search.action.count.CountRequest;
import org.server.search.action.delete.DeleteRequest;
import org.server.search.action.deletebyquery.DeleteByQueryRequest;
import org.server.search.action.get.GetRequest;
import org.server.search.action.index.IndexRequest;
import org.server.search.action.search.SearchRequest;
import org.server.search.action.search.SearchScrollRequest;

 
public class Requests {

    public static IndexRequest indexRequest(String index) {
        return new IndexRequest(index);
    }

    public static DeleteRequest deleteRequest(String index) {
        return new DeleteRequest(index);
    }

    public static DeleteByQueryRequest deleteByQueryRequest(String... indices) {
        return new DeleteByQueryRequest(indices);
    }

    public static GetRequest getRequest(String index) {
        return new GetRequest(index);
    }

    public static CountRequest countRequest(String... indices) {
        return new CountRequest(indices);
    }

    public static SearchRequest searchRequest(String... index) {
        return new SearchRequest(index);
    }

    public static SearchScrollRequest searchScrollRequest(String scrollId) {
        return new SearchScrollRequest(scrollId);
    }

    public static IndicesStatusRequest indicesStatus(String... indices) {
        return new IndicesStatusRequest(indices);
    }

    public static CreateIndexRequest createIndexRequest(String index) {
        return new CreateIndexRequest(index);
    }

    public static DeleteIndexRequest deleteIndexRequest(String index) {
        return new DeleteIndexRequest(index);
    }

    public static CreateMappingRequest createMappingRequest(String... indices) {
        return new CreateMappingRequest(indices);
    }

    public static RefreshRequest refreshRequest(String... indices) {
        return new RefreshRequest(indices);
    }

    public static FlushRequest flushRequest(String... indices) {
        return new FlushRequest(indices);
    }

    public static GatewaySnapshotRequest gatewaySnapshotRequest(String... indices) {
        return new GatewaySnapshotRequest(indices);
    }

    public static SinglePingRequest pingSingleRequest(String index) {
        return new SinglePingRequest(index);
    }

    public static BroadcastPingRequest pingBroadcastRequest(String... indices) {
        return new BroadcastPingRequest(indices);
    }

    public static ReplicationPingRequest pingReplicationRequest(String... indices) {
        return new ReplicationPingRequest(indices);
    }

    public static NodesInfoRequest nodesInfo() {
        return new NodesInfoRequest();
    }

    public static NodesInfoRequest nodesInfo(String... nodesIds) {
        return new NodesInfoRequest(nodesIds);
    }

    public static ClusterStateRequest clusterState() {
        return new ClusterStateRequest();
    }
}
