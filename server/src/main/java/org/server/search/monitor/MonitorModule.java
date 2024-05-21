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

package org.server.search.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryProvider;
import com.google.inject.multibindings.MapBinder;
import org.server.search.monitor.dump.DumpContributorFactory;
import org.server.search.monitor.dump.DumpMonitorService;
import org.server.search.monitor.dump.cluster.ClusterDumpContributor;
import org.server.search.monitor.dump.heap.HeapDumpContributor;
import org.server.search.monitor.dump.summary.SummaryDumpContributor;
import org.server.search.monitor.dump.thread.ThreadDumpContributor;
import org.server.search.monitor.jvm.JvmMonitorService;
import org.server.search.monitor.memory.MemoryMonitor;
import org.server.search.monitor.memory.MemoryMonitorService;
import org.server.search.monitor.memory.alpha.AlphaMemoryMonitor;
import org.server.search.util.settings.Settings;

import java.util.Map;

import static org.server.search.monitor.dump.cluster.ClusterDumpContributor.*;
import static org.server.search.monitor.dump.heap.HeapDumpContributor.*;
import static org.server.search.monitor.dump.summary.SummaryDumpContributor.*;
import static org.server.search.monitor.dump.thread.ThreadDumpContributor.*;

 
public class MonitorModule extends AbstractModule {

    public static final class MonitorSettings {
        public static final String MEMORY_MANAGER_TYPE = "monitor.memory.type";
    }

    private final Settings settings;

    public MonitorModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        bind(MemoryMonitor.class)
                .to(settings.getAsClass(MonitorSettings.MEMORY_MANAGER_TYPE, AlphaMemoryMonitor.class, "org.server.search.monitor.memory.", "MemoryMonitor"))
                .asEagerSingleton();
        bind(MemoryMonitorService.class).asEagerSingleton();

        bind(JvmMonitorService.class).asEagerSingleton();

        MapBinder<String, DumpContributorFactory> tokenFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, DumpContributorFactory.class);

        Map<String, Settings> dumpContSettings = settings.getGroups("monitor.dump");
        for (Map.Entry<String, Settings> entry : dumpContSettings.entrySet()) {
            String dumpContributorName = entry.getKey();
            Settings dumpContributorSettings = entry.getValue();

            Class<? extends DumpContributorFactory> type = dumpContributorSettings.getAsClass("type", null, "org.server.search.monitor.dump." + dumpContributorName + ".", "DumpContributor");
            if (type == null) {
                throw new IllegalArgumentException("Dump Contributor [" + dumpContributorName + "] must have a type associated with it");
            }
            tokenFilterBinder.addBinding(dumpContributorName).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, type)).in(Scopes.SINGLETON);
        }
        // add default
        if (!dumpContSettings.containsKey(SUMMARY)) {
            tokenFilterBinder.addBinding(SUMMARY).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, SummaryDumpContributor.class)).in(Scopes.SINGLETON);
        }
        if (!dumpContSettings.containsKey(THREAD_DUMP)) {
            tokenFilterBinder.addBinding(THREAD_DUMP).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, ThreadDumpContributor.class)).in(Scopes.SINGLETON);
        }
        if (!dumpContSettings.containsKey(HEAP_DUMP)) {
            tokenFilterBinder.addBinding(HEAP_DUMP).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, HeapDumpContributor.class)).in(Scopes.SINGLETON);
        }
        if (!dumpContSettings.containsKey(CLUSTER)) {
            tokenFilterBinder.addBinding(CLUSTER).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, ClusterDumpContributor.class)).in(Scopes.SINGLETON);
        }


        bind(DumpMonitorService.class).asEagerSingleton();
    }
}
