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

package org.server.search.client.transport;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.server.search.ElasticSearchException;
import org.server.search.action.ActionFuture;
import org.server.search.action.ActionListener;
import org.server.search.action.count.CountRequest;
import org.server.search.action.count.CountResponse;
import org.server.search.action.delete.DeleteRequest;
import org.server.search.action.delete.DeleteResponse;
import org.server.search.action.deletebyquery.DeleteByQueryRequest;
import org.server.search.action.deletebyquery.DeleteByQueryResponse;
import org.server.search.action.get.GetRequest;
import org.server.search.action.get.GetResponse;
import org.server.search.action.index.IndexRequest;
import org.server.search.action.index.IndexResponse;
import org.server.search.action.search.SearchRequest;
import org.server.search.action.search.SearchResponse;
import org.server.search.action.search.SearchScrollRequest;
import org.server.search.client.AdminClient;
import org.server.search.client.Client;
import org.server.search.client.transport.action.ClientTransportActionModule;
import org.server.search.client.transport.support.InternalTransportClient;
import org.server.search.cluster.ClusterNameModule;
import org.server.search.cluster.node.Node;
import org.server.search.env.Environment;
import org.server.search.env.EnvironmentModule;
import org.server.search.server.internal.InternalSettingsPerparer;
import org.server.search.threadpool.ThreadPoolModule;
import org.server.search.transport.TransportModule;
import org.server.search.transport.TransportService;
import org.server.search.util.Tuple;
import org.server.search.util.settings.ImmutableSettings;
import org.server.search.util.settings.Settings;
import org.server.search.util.settings.SettingsModule;
import org.server.search.util.transport.TransportAddress;

import java.util.ArrayList;

import static org.server.search.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportClient implements Client {

    private final Injector injector;

    private final Settings settings;

    private final Environment environment;


    private final TransportClientNodesService nodesService;

    private final InternalTransportClient internalClient;


    public TransportClient() throws ElasticSearchException {
        this(Builder.EMPTY_SETTINGS, true);
    }

    public TransportClient(Settings settings) {
        this(settings, true);
    }

    public TransportClient(Settings pSettings, boolean loadConfigSettings) throws ElasticSearchException {
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(pSettings, loadConfigSettings);
        this.settings = settingsBuilder().putAll(tuple.v1())
                .putBoolean("network.server", false)
                .putBoolean("discovery.client", true)
                .build();
        this.environment = tuple.v2();

        ArrayList<Module> modules = new ArrayList<Module>();
        modules.add(new EnvironmentModule(environment));
        modules.add(new SettingsModule(settings));
        modules.add(new ClusterNameModule(settings));
        modules.add(new ThreadPoolModule(settings));
        modules.add(new TransportModule(settings));
        modules.add(new ClientTransportActionModule());
        modules.add(new ClientTransportModule());

        // disabled, still having problems with jgroups acting just as client
        if (settings.getAsBoolean("discovery.enabled", true) && false) {
            modules.add(new TransportClientClusterModule(settings));
        }

        injector = Guice.createInjector(modules);

        injector.getInstance(TransportService.class).start();
        try {
            injector.getInstance(TransportClientClusterService.class).start();
        } catch (Exception e) {
            // ignore
        }

        nodesService = injector.getInstance(TransportClientNodesService.class);
        internalClient = injector.getInstance(InternalTransportClient.class);
    }

    /**
     * Returns the current registered transport addresses to use (added using
     * {@link #addTransportAddress(TransportAddress)}.
     */
    public ImmutableList<TransportAddress> transportAddresses() {
        return nodesService.transportAddresses();
    }

    /**
     * Returns the current connected transport nodes that this client will use.
     *
     * <p>The nodes include all the nodes that are currently alive based on the transport
     * addresses provided.
     */
    public ImmutableList<Node> connectedNodes() {
        return nodesService.connectedNodes();
    }

    /**
     * Adds a transport address that will be used to connect to.
     *
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     *
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public TransportClient addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddress(transportAddress);
        return this;
    }

    /**
     * Removes a transport address from the list of transport addresses that are used to connect to.
     */
    public TransportClient removeTransportAddress(TransportAddress transportAddress) {
        nodesService.removeTransportAddress(transportAddress);
        return this;
    }

    /**
     * Closes the client.
     */
    @Override public void close() {
        try {
            injector.getInstance(TransportClientClusterService.class).close();
        } catch (Exception e) {
            // ignore
        }
        injector.getInstance(TransportClientNodesService.class).close();
        injector.getInstance(TransportService.class).close();
    }

    @Override public AdminClient admin() {
        return internalClient.admin();
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request) {
        return internalClient.index(request);
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request, ActionListener<IndexResponse> listener) {
        return internalClient.index(request, listener);
    }

    @Override public void execIndex(IndexRequest request, ActionListener<IndexResponse> listener) {
        internalClient.execIndex(request, listener);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return internalClient.delete(request);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        return internalClient.delete(request, listener);
    }

    @Override public void execDelete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        internalClient.execDelete(request, listener);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return internalClient.deleteByQuery(request);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        return internalClient.deleteByQuery(request, listener);
    }

    @Override public void execDeleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        internalClient.execDeleteByQuery(request, listener);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request) {
        return internalClient.get(request);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request, ActionListener<GetResponse> listener) {
        return internalClient.get(request, listener);
    }

    @Override public void execGet(GetRequest request, ActionListener<GetResponse> listener) {
        internalClient.execGet(request, listener);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request) {
        return internalClient.count(request);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request, ActionListener<CountResponse> listener) {
        return internalClient.count(request, listener);
    }

    @Override public void execCount(CountRequest request, ActionListener<CountResponse> listener) {
        internalClient.execCount(request, listener);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request) {
        return internalClient.search(request);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request, ActionListener<SearchResponse> listener) {
        return internalClient.search(request, listener);
    }

    @Override public void execSearch(SearchRequest request, ActionListener<SearchResponse> listener) {
        internalClient.execSearch(request, listener);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return internalClient.searchScroll(request);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        return internalClient.searchScroll(request, listener);
    }

    @Override public void execSearchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        internalClient.execSearchScroll(request, listener);
    }
}
