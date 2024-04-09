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

package org.server.search.server.internal;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.server.search.ElasticSearchException;
import org.server.search.Version;
import org.server.search.action.TransportActionModule;
import org.server.search.client.Client;
import org.server.search.client.server.ServerClientModule;
import org.server.search.cluster.ClusterModule;
import org.server.search.cluster.ClusterNameModule;
import org.server.search.cluster.ClusterService;
import org.server.search.cluster.routing.RoutingService;
import org.server.search.discovery.DiscoveryModule;
import org.server.search.discovery.DiscoveryService;
import org.server.search.env.Environment;
import org.server.search.env.EnvironmentModule;
import org.server.search.gateway.GatewayModule;
import org.server.search.gateway.GatewayService;
import org.server.search.http.HttpServer;
import org.server.search.http.HttpServerModule;
import org.server.search.index.store.fs.FsStores;
import org.server.search.indices.IndicesModule;
import org.server.search.indices.IndicesService;
import org.server.search.jmx.JmxModule;
import org.server.search.jmx.JmxService;
import org.server.search.monitor.MonitorModule;
import org.server.search.monitor.MonitorService;
import org.server.search.search.SearchModule;
import org.server.search.search.SearchService;
import org.server.search.server.Server;
import org.server.search.threadpool.ThreadPool;
import org.server.search.threadpool.ThreadPoolModule;
import org.server.search.transport.TransportModule;
import org.server.search.transport.TransportService;
import org.server.search.util.Tuple;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.guice.Injectors;
import org.server.search.util.io.FileSystemUtils;
import org.server.search.util.logging.Loggers;
import org.server.search.util.settings.Settings;
import org.server.search.util.settings.SettingsModule;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.server.search.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public final class InternalServer implements Server {

    private final Lifecycle lifecycle = new Lifecycle();

    private final Injector injector;

    private final Settings settings;

    private final Environment environment;

    private final Client client;

    public InternalServer() throws ElasticSearchException {
        this(Builder.EMPTY_SETTINGS, true);
    }

    public InternalServer(Settings pSettings, boolean loadConfigSettings) throws ElasticSearchException {
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(pSettings, loadConfigSettings);
        this.settings = tuple.v1();
        this.environment = tuple.v2();

        Logger logger = Loggers.getLogger(Server.class, settings.get("name"));
        logger.info("{{}}: Initializing ...", Version.full());

        ArrayList<Module> modules = new ArrayList<Module>();
        modules.add(new JmxModule(settings));
        modules.add(new EnvironmentModule(environment));
        modules.add(new ClusterNameModule(settings));
        modules.add(new SettingsModule(settings));
        modules.add(new ThreadPoolModule(settings));
        modules.add(new DiscoveryModule(settings));
        modules.add(new ClusterModule(settings));
        modules.add(new TransportModule(settings));
        if (settings.getAsBoolean("http.enabled", true)) {
            modules.add(new HttpServerModule(settings));
        }
        modules.add(new IndicesModule(settings));
        modules.add(new SearchModule());
        modules.add(new TransportActionModule());
        modules.add(new MonitorModule(settings));
        modules.add(new GatewayModule(settings));
        modules.add(new ServerClientModule());


        injector = Guice.createInjector(modules);

        client = injector.getInstance(Client.class);

        logger.info("{{}}: Initialized", Version.full());
    }

    @Override public Settings settings() {
        return this.settings;
    }

    @Override public Client client() {
        return client;
    }

    public Server start() {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        Logger logger = Loggers.getLogger(Server.class, settings.get("name"));
        logger.info("{{}}: Starting ...", Version.full());

        injector().getInstance(IndicesService.class).start();
        injector().getInstance(GatewayService.class).start();
        injector.getInstance(ClusterService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector().getInstance(MonitorService.class).start();
        injector.getInstance(TransportService.class).start();
        DiscoveryService discoService = injector.getInstance(DiscoveryService.class).start();
        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).start();
        }
        injector.getInstance(JmxService.class).connectAndRegister(discoService.nodeDescription());

        logger.info("{{}}: Started", Version.full());

        return this;
    }

    @Override public Server stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        Logger logger = Loggers.getLogger(Server.class, settings.get("name"));
        logger.info("{ElasticSearch/{}}: Stopping ...", Version.full());

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).stop();
        }
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(DiscoveryService.class).stop();
        injector.getInstance(MonitorService.class).stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(IndicesService.class).stop();
        injector.getInstance(TransportService.class).stop();
        injector.getInstance(JmxService.class).close();

        // Not pretty, but here we go
        try {
            FileSystemUtils.deleteRecursively(new File(new File(environment.workWithClusterFile(), FsStores.DEFAULT_INDICES_LOCATION),
                    injector.getInstance(ClusterService.class).state().nodes().localNodeId()));
        } catch (Exception e) {
            // ignore
        }

        Injectors.close(injector);

        logger.info("{{}}: Stopped", Version.full());

        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }

        Logger logger = Loggers.getLogger(Server.class, settings.get("name"));
        logger.info("{{}}: Closing ...", Version.full());

        if (settings.getAsBoolean("http.enabled", true)) {
            injector.getInstance(HttpServer.class).close();
        }
        injector.getInstance(Client.class).close();
        injector.getInstance(RoutingService.class).close();
        injector.getInstance(ClusterService.class).close();
        injector.getInstance(DiscoveryService.class).close();
        injector.getInstance(MonitorService.class).close();
        injector.getInstance(GatewayService.class).close();
        injector.getInstance(SearchService.class).close();
        injector.getInstance(IndicesService.class).close();
        injector.getInstance(TransportService.class).close();

        injector.getInstance(ThreadPool.class).shutdown();
        try {
            injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            injector.getInstance(ThreadPool.class).shutdownNow();
        } catch (Exception e) {
            // ignore
        }

        logger.info("{{}}: Closed", Version.full());
    }

    public Injector injector() {
        return this.injector;
    }

    public static void main(String[] args) throws Exception {
        final InternalServer server = new InternalServer();
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                server.close();
            }
        });
    }
}