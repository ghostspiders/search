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

package org.server.search.index.gateway.none;

import com.google.inject.Inject;
import org.apache.lucene.index.IndexCommit;
import org.server.search.index.deletionpolicy.SnapshotIndexCommit;
import org.server.search.index.gateway.IndexShardGateway;
import org.server.search.index.gateway.IndexShardGatewayRecoveryException;
import org.server.search.index.gateway.RecoveryStatus;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.AbstractIndexShardComponent;
import org.server.search.index.shard.IndexShard;
import org.server.search.index.shard.InternalIndexShard;
import org.server.search.index.shard.ShardId;
import org.server.search.index.translog.Translog;
import org.server.search.util.SizeUnit;
import org.server.search.util.SizeValue;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;

/**
 * @author kimchy (Shay Banon)
 */
public class NoneIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private final InternalIndexShard indexShard;

    @Inject public NoneIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, IndexShard indexShard) {
        super(shardId, indexSettings);
        this.indexShard = (InternalIndexShard) indexShard;
    }

    @Override public RecoveryStatus recover() throws IndexShardGatewayRecoveryException {
        // in the none case, we simply start the shard
        indexShard.start();
        return new RecoveryStatus(new RecoveryStatus.Index(0, new SizeValue(0, SizeUnit.BYTES)), new RecoveryStatus.Translog(0, new SizeValue(0, SizeUnit.BYTES)));
    }

    @Override public void snapshot(SnapshotIndexCommit snapshotIndexCommit, Translog.Snapshot translogSnapshot) {
        // nothing to do here
    }

    @Override public boolean requiresSnapshotScheduling() {
        return false;
    }

    @Override public void close() {
    }

}
