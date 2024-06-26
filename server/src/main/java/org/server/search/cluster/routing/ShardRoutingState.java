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

package org.server.search.cluster.routing;

import org.server.search.SearchIllegalStateException;

 
public enum ShardRoutingState {
    UNASSIGNED((byte) 1), INITIALIZING((byte) 2), STARTED((byte) 3), RELOCATING((byte) 4);

    private byte value;

    ShardRoutingState(byte value) {
        this.value = value;
    }

    public byte value() {
        return this.value;
    }

    public static ShardRoutingState fromValue(byte value) {
        switch (value) {
            case 1:
                return UNASSIGNED;
            case 2:
                return INITIALIZING;
            case 3:
                return STARTED;
            case 4:
                return RELOCATING;
            default:
                throw new SearchIllegalStateException("No should routing state mapped for [" + value + "]");
        }
    }
}
