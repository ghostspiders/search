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

package org.server.search.action;

import org.server.search.cluster.ClusterState;

 
public class Actions {

    public static String[] processIndices(ClusterState state, String[] indices) {
        if (indices == null || indices.length == 0) {
            return state.routingTable().indicesRouting().keySet().toArray(new String[state.routingTable().indicesRouting().keySet().size()]);
        }
        if (indices.length == 1) {
            if (indices[0].length() == 0) {
                return state.routingTable().indicesRouting().keySet().toArray(new String[state.routingTable().indicesRouting().keySet().size()]);
            }
            if (indices[0].equals("_all")) {
                return state.routingTable().indicesRouting().keySet().toArray(new String[state.routingTable().indicesRouting().keySet().size()]);
            }
        }
        return indices;
    }

    public static ActionRequestValidationException addValidationError(String error, ActionRequestValidationException validationException) {
        if (validationException == null) {
            validationException = new ActionRequestValidationException();
        }
        validationException.addValidationError(error);
        return validationException;
    }
}
