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

package org.server.search.index.deletionpolicy;

import com.google.inject.Inject;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.AbstractIndexShardComponent;
import org.server.search.index.shard.IndexShardLifecycle;
import org.server.search.index.shard.ShardId;
import org.server.search.util.settings.Settings;

import java.util.List;

/**
 * This {@link IndexDeletionPolicy} implementation that
 * keeps only the most recent commit and immediately removes
 * all prior commits after a new commit is done.  This is
 * the default deletion policy.
 */
@IndexShardLifecycle
public class KeepOnlyLastDeletionPolicy extends AbstractIndexShardComponent{

    @Inject public KeepOnlyLastDeletionPolicy(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
        logger.debug("Using [KeepOnlyLast] deletion policy");
    }

    /**
     * Deletes all commits except the most recent one.
     */
    public void onInit(List<? extends IndexCommit> commits) {
        // Note that commits.size() should normally be 1:
        onCommit(commits);
    }

    /**
     * Deletes all commits except the most recent one.
     */
    public void onCommit(List<? extends IndexCommit> commits) {
        // Note that commits.size() should normally be 2 (if not
        // called by onInit above):
        int size = commits.size();
        for (int i = 0; i < size - 1; i++) {
            commits.get(i).delete();
        }
    }
}
