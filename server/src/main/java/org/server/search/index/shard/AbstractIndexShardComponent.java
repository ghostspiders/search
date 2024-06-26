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

package org.server.search.index.shard;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.server.search.index.settings.IndexSettings;
import org.server.search.jmx.ManagedGroupName;
import org.server.search.util.logging.Loggers;
import org.server.search.util.settings.Settings;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

import static org.server.search.index.shard.IndexShardManagement.*;


public abstract class AbstractIndexShardComponent extends IndexDeletionPolicy implements IndexShardComponent {

    protected final Logger logger;

    protected final ShardId shardId;

    protected final Settings indexSettings;

    protected final Settings componentSettings;

    protected AbstractIndexShardComponent(ShardId shardId, @IndexSettings Settings indexSettings) {
        this.shardId = shardId;
        this.indexSettings = indexSettings;
        this.componentSettings = indexSettings.getComponentSettings(getClass());

        this.logger = Loggers.getLogger(getClass(), indexSettings, shardId);
    }

    @Override public ShardId shardId() {
        return this.shardId;
    }

    @Override public Settings indexSettings() {
        return this.indexSettings;
    }

    public String nodeName() {
        return indexSettings.get("name", "");
    }

    @ManagedGroupName
    public String managementGroupName() {
        return buildShardGroupName(shardId);
    }
    @Override
    public  void onInit(List<? extends IndexCommit> commits) throws IOException{
        onCommit(commits);
    }

    @Override
    public  void onCommit(List<? extends IndexCommit> commits) throws IOException{
        int size = commits.size();
        for (int i = 0; i < size - 1; i++) {
            commits.get(i).delete();
        }
    }
}
