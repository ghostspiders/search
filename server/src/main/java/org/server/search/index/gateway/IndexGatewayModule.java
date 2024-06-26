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

package org.server.search.index.gateway;

import com.google.inject.AbstractModule;
import org.server.search.gateway.Gateway;
import org.server.search.util.guice.ModulesFactory;
import org.server.search.util.settings.Settings;

 
public class IndexGatewayModule extends AbstractModule {

    private final Settings settings;

    private final Gateway gateway;

    public IndexGatewayModule(Settings settings, Gateway gateway) {
        this.settings = settings;
        this.gateway = gateway;
    }

    @Override protected void configure() {
        ModulesFactory.createModule(settings.getAsClass("index.gateway.type", gateway.suggestIndexGateway(), "org.server.search.index.gateway.", "IndexGatewayModule"), settings).configure(binder());
    }
}
