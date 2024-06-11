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

package org.server.search.action.search;

import org.server.search.SearchIllegalArgumentException;


public enum SearchOperationThreading {
    /**
     * 不使用线程。所有本地分片的操作都将在调用线程上同步执行。
     * 适用于单线程环境或操作非常快速且不需要并行处理的场景。
     */
    NO_THREADS((byte) 0),
    /**
     * 单线程模型。所有本地分片的操作将在单个派生的线程上串行执行。
     * 这种方式可以防止调用线程被阻塞，同时保持操作的顺序性。
     */
    SINGLE_THREAD((byte) 1),
    /**
     * 每个分片一个线程模型。每个本地分片的操作都将在自己的线程上独立执行。
     * 这种方式可以提高并行处理的性能，尤其是在多核处理器上。
     * 但需要注意管理线程间的资源竞争和同步问题。
     */
    THREAD_PER_SHARD((byte) 2);

    private final byte id;

    SearchOperationThreading(byte id) {
        this.id = id;
    }

    public byte id() {
        return this.id;
    }

    public static SearchOperationThreading fromId(byte id) {
        if (id == 0) {
            return NO_THREADS;
        }
        if (id == 1) {
            return SINGLE_THREAD;
        }
        if (id == 2) {
            return THREAD_PER_SHARD;
        }
        throw new SearchIllegalArgumentException("No type matching id [" + id + "]");
    }

    public static SearchOperationThreading fromString(String value, SearchOperationThreading defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return SearchOperationThreading.valueOf(value.toUpperCase());
    }
}