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

import org.server.search.util.IdentityHashSet;

import java.util.Iterator;
import java.util.Set;

 
public class GroupShardsIterator implements Iterable<ShardsIterator> {

    private final Set<ShardsIterator> iterators;

    public GroupShardsIterator() {
        this(new IdentityHashSet<ShardsIterator>());
    }

    public GroupShardsIterator(Set<ShardsIterator> iterators) {
        this.iterators = iterators;
    }

    public void add(ShardsIterator shardsIterator) {
        iterators.add(shardsIterator);
    }

    public void add(Iterable<ShardsIterator> shardsIterator) {
        for (ShardsIterator it : shardsIterator) {
            add(it);
        }
    }

    public int totalSize() {
        int size = 0;
        for (ShardsIterator shard : iterators) {
            size += shard.size();
        }
        return size;
    }

    public int size() {
        return iterators.size();
    }

    public Set<ShardsIterator> iterators() {
        return iterators;
    }

    @Override public Iterator<ShardsIterator> iterator() {
        return iterators.iterator();
    }
}
