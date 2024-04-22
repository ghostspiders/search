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

package org.server.search.index.store.support;

import org.apache.lucene.store.Directory;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.AbstractIndexShardComponent;
import org.server.search.index.shard.ShardId;
import org.server.search.index.store.Store;
import org.server.search.util.SizeValue;
import org.server.search.util.lucene.Directories;
import org.server.search.util.settings.Settings;

import java.io.IOException;

/**
 * 
 */
public abstract class AbstractStore<T extends Directory> extends AbstractIndexShardComponent implements Store<T> {

    protected AbstractStore(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    @Override public void deleteContent() throws IOException {
        Directories.deleteFiles(directory());
    }

    @Override public void fullDelete() throws IOException {
        deleteContent();
    }

    @Override public SizeValue estimateSize() throws IOException {
        return Directories.estimateSize(directory());
    }

    /**
     * Returns <tt>true</tt> by default.
     */
    @Override public boolean suggestUseCompoundFile() {
        return true;
    }

    @Override public void close() throws IOException {
        directory().close();
    }
}
