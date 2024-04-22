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

package org.server.search.discovery;

import com.google.inject.Inject;
import org.server.search.SearchException;
import org.server.search.cluster.ClusterState;
import org.server.search.util.TimeValue;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.component.LifecycleComponent;
import org.server.search.util.settings.Settings;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 
 */
public class DiscoveryService extends AbstractComponent implements LifecycleComponent<DiscoveryService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final TimeValue initialStateTimeout;

    private final Discovery discovery;

    @Inject public DiscoveryService(Settings settings, Discovery discovery) {
        super(settings);
        this.discovery = discovery;
        this.initialStateTimeout = componentSettings.getAsTime("initialStateTimeout", TimeValue.timeValueSeconds(30));
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override public DiscoveryService start() throws Exception {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        final CountDownLatch latch = new CountDownLatch(1);
        InitialStateDiscoveryListener listener = new InitialStateDiscoveryListener() {
            @Override public void initialStateProcessed() {
                latch.countDown();
            }
        };
        discovery.addListener(listener);
        try {
            discovery.start();
            try {
                logger.trace("Waiting for {} for the initial state to be set by the discovery", initialStateTimeout);
                if (latch.await(initialStateTimeout.millis(), TimeUnit.MILLISECONDS)) {
                    logger.trace("Initial state set from discovery");
                } else {
                    logger.warn("Waited for {} and no initial state was set by the discovery", initialStateTimeout);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        } finally {
            discovery.removeListener(listener);
        }
        logger.info(discovery.nodeDescription());
        return this;
    }

    @Override public DiscoveryService stop() throws SearchException, InterruptedException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        discovery.stop();
        return this;
    }

    @Override public void close() throws SearchException, InterruptedException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        discovery.close();
    }

    public String nodeDescription() {
        return discovery.nodeDescription();
    }

    public boolean firstMaster() {
        return discovery.firstMaster();
    }

    public void publish(ClusterState clusterState) {
        if (!lifecycle.started()) {
            return;
        }
        discovery.publish(clusterState);
    }
}
