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

package org.server.search.index.merge.scheduler;

import com.google.inject.Inject;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.MergeScheduler;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.AbstractIndexShardComponent;
import org.server.search.index.shard.IndexShardLifecycle;
import org.server.search.index.shard.ShardId;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;


@IndexShardLifecycle
public class ConcurrentMergeSchedulerProvider extends AbstractIndexShardComponent implements MergeSchedulerProvider {

    private final int maxThreadCount;

    @Inject public ConcurrentMergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);

        this.maxThreadCount = componentSettings.getAsInt("maxThreadCount", 1);
        logger.debug("Using [concurrent] merge scheduler with maxThreadCount[{}]", maxThreadCount);
    }

    @Override public MergeScheduler newMergeScheduler() {
        ConcurrentMergeScheduler concurrentMergeScheduler = new ConcurrentMergeScheduler();
        return concurrentMergeScheduler;
    }


}
