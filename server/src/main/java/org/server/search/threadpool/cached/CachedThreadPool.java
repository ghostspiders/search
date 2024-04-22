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

package org.server.search.threadpool.cached;

import com.google.inject.Inject;
import org.server.search.threadpool.support.AbstractThreadPool;
import org.server.search.util.TimeValue;
import org.server.search.util.concurrent.DynamicExecutors;
import org.server.search.util.settings.Settings;

import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.server.search.util.TimeValue.*;
import static org.server.search.util.settings.ImmutableSettings.Builder.*;

/**
 * 
 */
public class CachedThreadPool extends AbstractThreadPool {

    private final TimeValue keepAlive;

    private final int scheduledSize;

    public CachedThreadPool() {
        this(EMPTY_SETTINGS);
    }

    @Inject public CachedThreadPool(Settings settings) {
        super(settings);
        this.scheduledSize = componentSettings.getAsInt("scheduledSize", 20);
        this.keepAlive = componentSettings.getAsTime("keepAlive", timeValueSeconds(60));
        logger.debug("Initializing {} thread pool with keepAlive[{}], scheduledSize[{}]", new Object[]{getType(), keepAlive, scheduledSize});
        executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                keepAlive.millis(), TimeUnit.MILLISECONDS,
                new SynchronousQueue<Runnable>(),
                DynamicExecutors.daemonThreadFactory(settings, "[tp]"));
        scheduledExecutorService = Executors.newScheduledThreadPool(scheduledSize, DynamicExecutors.daemonThreadFactory(settings, "[sc]"));
        started = true;
    }

    @Override public String getType() {
        return "cached";
    }
}
