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

package org.server.search.action.admin.cluster.state;

import org.server.search.action.ActionRequestValidationException;
import org.server.search.action.support.master.MasterNodeOperationRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class ClusterStateRequest extends MasterNodeOperationRequest {

    public ClusterStateRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        return null;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
    }

    @Override public void writeTo(DataOutput out) throws IOException {
    }
}