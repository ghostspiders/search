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

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.monitor.jvm.JvmMonitorService;
import org.server.search.monitor.memory.MemoryMonitorService;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.settings.Settings;

/**
 * 
 */
public class MonitorService extends AbstractComponent implements LifecycleComponent<MonitorService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final MemoryMonitorService memoryMonitorService;

    private final JvmMonitorService jvmMonitorService;

    @Inject public MonitorService(Settings settings, MemoryMonitorService memoryMonitorService, JvmMonitorService jvmMonitorService) {
        super(settings);
        this.memoryMonitorService = memoryMonitorService;
        this.jvmMonitorService = jvmMonitorService;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public MonitorService start() throws Exception {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        memoryMonitorService.start();
        jvmMonitorService.start();
        return this;
    }

    @Override public MonitorService stop() throws SearchException, InterruptedException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        memoryMonitorService.stop();
        jvmMonitorService.stop();
        return this;
    }

    public void close() throws InterruptedException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        memoryMonitorService.close();
        jvmMonitorService.close();
    }
}
