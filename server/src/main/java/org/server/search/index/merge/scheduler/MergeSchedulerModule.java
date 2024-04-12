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

import com.google.inject.AbstractModule;
import org.server.search.index.shard.IndexShardLifecycle;
import org.server.search.util.settings.Settings;

import static org.server.search.index.merge.scheduler.MergeSchedulerModule.MergeSchedulerSettings.*;
import static org.server.search.index.merge.scheduler.MergeSchedulerModule.MergeSchedulerSettings.TYPE;

/**
 * @author kimchy (Shay Banon)
 */
@IndexShardLifecycle
public class MergeSchedulerModule extends AbstractModule {

    public static class MergeSchedulerSettings {
        public static final String TYPE = "index.merge.scheduler.type";
    }

    private final Settings settings;

    public MergeSchedulerModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        bind(MergeSchedulerProvider.class)
                .to(settings.getAsClass(TYPE, ConcurrentMergeSchedulerProvider.class))
                .asEagerSingleton();
    }
}