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

package org.server.search.index.store.memory;

import com.google.inject.Inject;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.ShardId;
import org.server.search.index.store.support.AbstractStore;
import org.server.search.util.SizeUnit;
import org.server.search.util.SizeValue;
import org.server.search.util.settings.Settings;


public class MemoryStore extends AbstractStore<MemoryDirectory> {

    private final SizeValue bufferSize;

    private final SizeValue cacheSize;

    private final boolean warmCache;

    private MemoryDirectory directory;

    @Inject public MemoryStore(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);

        this.bufferSize = componentSettings.getAsSize("bufferSize", new SizeValue(1, SizeUnit.KB));
        this.cacheSize = componentSettings.getAsSize("cacheSize", new SizeValue(20, SizeUnit.MB));
        this.warmCache = componentSettings.getAsBoolean("warmCache", true);

        this.directory = new MemoryDirectory(bufferSize, cacheSize, warmCache);
        logger.debug("Using [Memory] Store with bufferSize[{}], cacheSize[{}], warmCache[{}]",
                new Object[]{directory.bufferSize(), directory.cacheSize(), warmCache});
    }

    @Override public MemoryDirectory directory() {
        return directory;
    }

    /**
     * Its better to not use the compound format when using the Ram store.
     */
    @Override public boolean suggestUseCompoundFile() {
        return false;
    }
}