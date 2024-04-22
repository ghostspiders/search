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

package org.server.search.action.count;

import org.server.search.action.support.broadcast.BroadcastShardOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class ShardCountResponse extends BroadcastShardOperationResponse {

    private long count;

    ShardCountResponse() {

    }

    public ShardCountResponse(String index, int shardId, long count) {
        super(index, shardId);
        this.count = count;
    }

    public long count() {
        return this.count;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        count = in.readLong();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(count);
    }
}
