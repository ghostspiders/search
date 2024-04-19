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

package org.server.search.index.store.fs;

import org.apache.lucene.store.FSDirectory;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.ShardId;
import org.server.search.index.store.support.AbstractStore;
import org.server.search.util.io.FileSystemUtils;
import org.server.search.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractFsStore<T extends FSDirectory> extends AbstractStore<T> {

    public AbstractFsStore(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    @Override public void fullDelete() throws IOException {
        FileSystemUtils.deleteRecursively(directory().getDirectory().toFile());
        // if we are the last ones, delete also the actual index
        if (directory().getDirectory().toFile().getParentFile().list().length == 0) {
            FileSystemUtils.deleteRecursively(directory().getDirectory().toFile().getParentFile());
        }
    }
}
