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

package org.server.search.gateway.none;

import com.google.inject.Module;
import org.server.search.SearchException;
import org.server.search.cluster.metadata.MetaData;
import org.server.search.gateway.Gateway;
import org.server.search.gateway.GatewayException;
import org.server.search.index.gateway.none.NoneIndexGatewayModule;
import org.server.search.util.component.Lifecycle;

 
public class NoneGateway implements Gateway {

    private final Lifecycle lifecycle = new Lifecycle();

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public Gateway start() throws SearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        return this;
    }

    @Override public Gateway stop() throws SearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        return this;
    }

    @Override public void close() throws SearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    @Override public void write(MetaData metaData) throws GatewayException {

    }

    @Override public MetaData read() throws GatewayException {
        return null;
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return NoneIndexGatewayModule.class;
    }

    @Override public void reset() {
    }
}
