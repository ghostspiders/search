/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.server.search.discovery.coordination;

import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.cluster.ClusterChangedEvent;
import org.server.search.discovery.Discovery;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.settings.Settings;

public class Coordinator extends AbstractComponent implements Discovery {


    public Coordinator(Settings settings) {
        super(settings);
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {

    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public Discovery start() throws Exception {
        return null;
    }

    @Override
    public Discovery stop() throws SearchException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws SearchException, InterruptedException {

    }
}
