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

package org.server.search.index.store.bytebuffer;

import com.google.inject.Inject;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.ShardId;
import org.server.search.index.store.support.AbstractStore;
import org.server.search.util.SizeUnit;
import org.server.search.util.SizeValue;
import org.server.search.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class ByteBufferStore extends AbstractStore<ByteBuffersDirectory> {

    private final SizeValue bufferSize;

    private final SizeValue cacheSize;

    private final boolean direct;

    private final boolean warmCache;

    private final ByteBuffersDirectory directory;

    @Inject public ByteBufferStore(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);

        this.bufferSize = componentSettings.getAsSize("bufferSize", new SizeValue(1, SizeUnit.KB));
        this.cacheSize = componentSettings.getAsSize("cacheSize", new SizeValue(20, SizeUnit.MB));
        this.direct = componentSettings.getAsBoolean("direct", true);
        this.warmCache = componentSettings.getAsBoolean("warmCache", true);
        this.directory = new ByteBuffersDirectory();
    }

    @Override public ByteBuffersDirectory directory() {
        return directory;
    }

    /**
     * Its better to not use the compound format when using the Ram store.
     */
    @Override public boolean suggestUseCompoundFile() {
        return false;
    }
}
