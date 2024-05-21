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

package org.server.search.action.deletebyquery;

import org.server.search.action.ActionResponse;
import org.server.search.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

 
public class DeleteByQueryResponse implements ActionResponse, Streamable {

    private Map<String, IndexDeleteByQueryResponse> indexResponses = new HashMap<String, IndexDeleteByQueryResponse>();

    DeleteByQueryResponse() {

    }

    public Map<String, IndexDeleteByQueryResponse> indices() {
        return indexResponses;
    }

    public IndexDeleteByQueryResponse index(String index) {
        return indexResponses.get(index);
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IndexDeleteByQueryResponse response = new IndexDeleteByQueryResponse();
            response.readFrom(in);
            indexResponses.put(response.index(), response);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(indexResponses.size());
        for (IndexDeleteByQueryResponse indexResponse : indexResponses.values()) {
            indexResponse.writeTo(out);
        }
    }
}
