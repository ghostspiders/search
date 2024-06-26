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

package org.server.search.index.shard;

import org.server.search.SearchIllegalArgumentException;

 
public enum IndexShardState {
    CREATED((byte) 0),
    RECOVERING((byte) 1),
    STARTED((byte) 2),
    RELOCATED((byte) 3),
    CLOSED((byte) 4);

    private final byte id;

    IndexShardState(byte id) {
        this.id = id;
    }

    public byte id() {
        return this.id;
    }

    public static IndexShardState fromId(byte id) throws SearchIllegalArgumentException {
        if (id == 0) {
            return CREATED;
        } else if (id == 1) {
            return RECOVERING;
        } else if (id == 2) {
            return STARTED;
        } else if (id == 3) {
            return RELOCATED;
        } else if (id == 4) {
            return CLOSED;
        }
        throw new SearchIllegalArgumentException("No mapping for id [" + id + "]");
    }
}
