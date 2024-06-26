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

package org.server.search.gateway;

import com.google.inject.AbstractModule;
import org.server.search.gateway.none.NoneGatewayModule;
import org.server.search.util.guice.ModulesFactory;
import org.server.search.util.settings.Settings;

 
public class GatewayModule extends AbstractModule {

    private final Settings settings;

    public GatewayModule(Settings settings) {
        this.settings = settings;
    }

    @Override protected void configure() {
        ModulesFactory.createModule(settings.getAsClass("gateway.type", NoneGatewayModule.class, "org.server.search.gateway.", "GatewayModule"), settings).configure(binder());
        bind(GatewayService.class).asEagerSingleton();
    }
}
