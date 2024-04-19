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

package org.server.search.indices;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.server.search.ElasticSearchException;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.routing.GroupShardsIterator;
import org.server.search.gateway.Gateway;
import org.server.search.index.*;
import org.server.search.index.analysis.AnalysisModule;
import org.server.search.index.analysis.AnalysisService;
import org.server.search.index.gateway.IndexGateway;
import org.server.search.index.gateway.IndexGatewayModule;
import org.server.search.index.mapper.MapperServiceModule;
import org.server.search.index.query.IndexQueryParserModule;
import org.server.search.index.routing.OperationRoutingModule;
import org.server.search.index.settings.IndexSettingsModule;
import org.server.search.index.similarity.SimilarityModule;
import org.server.search.indices.cluster.IndicesClusterStateService;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.concurrent.ThreadSafe;
import org.server.search.util.guice.Injectors;
import org.server.search.util.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.*;
import static com.google.common.collect.Sets.*;
import static org.server.search.cluster.metadata.IndexMetaData.*;
import static org.server.search.util.MapBuilder.*;
import static org.server.search.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class InternalIndicesService extends AbstractComponent implements IndicesService {

    private final Lifecycle lifecycle = new Lifecycle();

    private final IndicesClusterStateService clusterStateService;

    private final Injector injector;

    private final Map<String, Injector> indicesInjectors = new HashMap<String, Injector>();

    private volatile ImmutableMap<String, IndexService> indices = ImmutableMap.of();

    @Inject public InternalIndicesService(Settings settings, IndicesClusterStateService clusterStateService, Injector injector) {
        super(settings);
        this.clusterStateService = clusterStateService;
        this.injector = injector;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public IndicesService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        clusterStateService.start();
        return this;
    }

    @Override public IndicesService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        clusterStateService.stop();
        for (String index : indices.keySet()) {
            deleteIndex(index, true);
        }
        return this;
    }

    public synchronized void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        clusterStateService.close();
    }

    /**
     * Returns <tt>true</tt> if changes (adding / removing) indices, shards and so on are allowed.
     */
    public boolean changesAllowed() {
        // we check on stop here since we defined stop when we delete the indices
        return lifecycle.started();
    }

    @Override public UnmodifiableIterator<IndexService> iterator() {
        return indices.values().iterator();
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public Set<String> indices() {
        return newHashSet(indices.keySet());
    }

    public IndexService indexService(String index) {
        return indices.get(index);
    }

    @Override public IndexService indexServiceSafe(String index) throws IndexMissingException {
        IndexService indexService = indexService(index);
        if (indexService == null) {
            throw new IndexMissingException(new Index(index));
        }
        return indexService;
    }

    @Override public GroupShardsIterator searchShards(ClusterState clusterState, String[] indexNames, String queryHint) throws ElasticSearchException {
        if (indexNames == null || indexNames.length == 0) {
            ImmutableMap<String, IndexService> indices = this.indices;
            indexNames = indices.keySet().toArray(new String[indices.keySet().size()]);
        }
        GroupShardsIterator its = new GroupShardsIterator();
        for (String index : indexNames) {
            its.add(indexServiceSafe(index).operationRouting().searchShards(clusterState, queryHint));
        }
        return its;
    }

    public synchronized IndexService createIndex(String sIndexName, Settings settings, String localNodeId) throws ElasticSearchException {
        Index index = new Index(sIndexName);
        if (indicesInjectors.containsKey(index.name())) {
            throw new IndexAlreadyExistsException(index);
        }

        logger.debug("Creating Index [{}], shards [{}]/[{}]", new Object[]{sIndexName, settings.get(SETTING_NUMBER_OF_SHARDS), settings.get(SETTING_NUMBER_OF_REPLICAS)});

        Settings indexSettings = settingsBuilder()
                .put("settingsType", "index")
                .putAll(this.settings)
                .putAll(settings)
                .classLoader(settings.getClassLoader())
                .globalSettings(settings.getGlobalSettings())
                .build();

        Injector indexInjector = injector.createChildInjector(
                new IndexNameModule(index),
                new LocalNodeIdModule(localNodeId),
                new IndexSettingsModule(indexSettings),
                new AnalysisModule(indexSettings),
                new SimilarityModule(indexSettings),
                new IndexQueryParserModule(indexSettings),
                new MapperServiceModule(),
                new IndexGatewayModule(indexSettings, injector.getInstance(Gateway.class)),
                new OperationRoutingModule(indexSettings),
                new IndexModule());

        indicesInjectors.put(index.name(), indexInjector);

        IndexService indexService = indexInjector.getInstance(IndexService.class);

        indices = newMapBuilder(indices).put(index.name(), indexService).immutableMap();

        return indexService;
    }

    public synchronized void deleteIndex(String index) throws ElasticSearchException {
        deleteIndex(index, false);
    }

    private synchronized void deleteIndex(String index, boolean internalClose) throws ElasticSearchException {
        Injector indexInjector = indicesInjectors.remove(index);
        if (indexInjector == null) {
            if (internalClose) {
                return;
            }
            throw new IndexMissingException(new Index(index));
        }
        if (!internalClose) {
            logger.debug("Deleting Index [{}]", index);
        }

        Map<String, IndexService> tmpMap = newHashMap(indices);
        IndexService indexService = tmpMap.remove(index);
        indices = ImmutableMap.copyOf(tmpMap);

        indexService.close();

        indexInjector.getInstance(AnalysisService.class).close();
        indexInjector.getInstance(IndexServiceManagement.class).close();

        if (!internalClose) {
            indexInjector.getInstance(IndexGateway.class).delete();
        }
        indexInjector.getInstance(IndexGateway.class).close();

        Injectors.close(injector);
    }
}