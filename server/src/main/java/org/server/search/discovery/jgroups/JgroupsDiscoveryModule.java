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

package org.server.search.discovery.jgroups;

import com.google.inject.AbstractModule;
import org.server.search.discovery.Discovery;
import org.server.search.util.settings.Settings;

/**
 * 
 */
public class JgroupsDiscoveryModule extends AbstractModule {

    private final Settings settings;

    public JgroupsDiscoveryModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        if (settings.getAsBoolean("discovery.client", false)) {
            bind(Discovery.class).to(JgroupsClientDiscovery.class).asEagerSingleton();
        } else {
            bind(Discovery.class).to(JgroupsDiscovery.class).asEagerSingleton();
        }
    }
}
