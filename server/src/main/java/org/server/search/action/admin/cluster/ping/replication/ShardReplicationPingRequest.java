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

package org.server.search.action.admin.cluster.ping.replication;

import org.server.search.action.support.replication.ShardReplicationOperationRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class ShardReplicationPingRequest extends ShardReplicationOperationRequest {

    private int shardId;

    public ShardReplicationPingRequest(IndexReplicationPingRequest request, int shardId) {
        this(request.index(), shardId);
        timeout = request.timeout();
    }

    public ShardReplicationPingRequest(String index, int shardId) {
        this.index = index;
        this.shardId = shardId;
    }

    ShardReplicationPingRequest() {
    }

    public int shardId() {
        return this.shardId;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        shardId = in.readInt();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(shardId);
    }
}