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

package org.server.search.action.delete;

import org.server.search.action.ActionRequestValidationException;
import org.server.search.action.support.replication.ShardReplicationOperationRequest;
import org.server.search.util.Required;
import org.server.search.util.TimeValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.server.search.action.Actions.*;


public class DeleteRequest extends ShardReplicationOperationRequest {

    private String type;
    private String id;

    public DeleteRequest(String index) {
        this.index = index;
    }

    public DeleteRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    DeleteRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    @Override public DeleteRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    @Override public DeleteRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    String type() {
        return type;
    }

    @Required public DeleteRequest type(String type) {
        this.type = type;
        return this;
    }

    String id() {
        return id;
    }

    @Required public DeleteRequest id(String id) {
        this.id = id;
        return this;
    }

    public DeleteRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        type = in.readUTF();
        id = in.readUTF();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(type);
        out.writeUTF(id);
    }
}