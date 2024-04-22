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

package org.server.search.client.server;

import com.google.inject.Inject;
import org.server.search.action.ActionFuture;
import org.server.search.action.ActionListener;
import org.server.search.action.count.CountRequest;
import org.server.search.action.count.CountResponse;
import org.server.search.action.count.TransportCountAction;
import org.server.search.action.delete.DeleteRequest;
import org.server.search.action.delete.DeleteResponse;
import org.server.search.action.delete.TransportDeleteAction;
import org.server.search.action.deletebyquery.DeleteByQueryRequest;
import org.server.search.action.deletebyquery.DeleteByQueryResponse;
import org.server.search.action.deletebyquery.TransportDeleteByQueryAction;
import org.server.search.action.get.GetRequest;
import org.server.search.action.get.GetResponse;
import org.server.search.action.get.TransportGetAction;
import org.server.search.action.index.IndexRequest;
import org.server.search.action.index.IndexResponse;
import org.server.search.action.index.TransportIndexAction;
import org.server.search.action.search.*;
import org.server.search.client.AdminClient;
import org.server.search.client.Client;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.Settings;

/**
 * 
 */
public class ServerClient extends AbstractComponent implements Client {

    private final ServerAdminClient admin;

    private final TransportIndexAction indexAction;

    private final TransportDeleteAction deleteAction;

    private final TransportDeleteByQueryAction deleteByQueryAction;

    private final TransportGetAction getAction;

    private final TransportCountAction countAction;

    private final TransportSearchAction searchAction;

    private final TransportSearchScrollAction searchScrollAction;

    @Inject public ServerClient(Settings settings, ServerAdminClient admin,
                                TransportIndexAction indexAction, TransportDeleteAction deleteAction,
                                TransportDeleteByQueryAction deleteByQueryAction, TransportGetAction getAction, TransportCountAction countAction,
                                TransportSearchAction searchAction, TransportSearchScrollAction searchScrollAction) {
        super(settings);
        this.admin = admin;
        this.indexAction = indexAction;
        this.deleteAction = deleteAction;
        this.deleteByQueryAction = deleteByQueryAction;
        this.getAction = getAction;
        this.countAction = countAction;
        this.searchAction = searchAction;
        this.searchScrollAction = searchScrollAction;
    }

    @Override public void close() {
        // nothing really to do
    }

    @Override public AdminClient admin() {
        return this.admin;
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request) {
        return indexAction.submit(request);
    }

    @Override public ActionFuture<IndexResponse> index(IndexRequest request, ActionListener<IndexResponse> listener) {
        return indexAction.submit(request, listener);
    }

    @Override public void execIndex(IndexRequest request, ActionListener<IndexResponse> listener) {
        indexAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return deleteAction.submit(request);
    }

    @Override public ActionFuture<DeleteResponse> delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        return deleteAction.submit(request, listener);
    }

    @Override public void execDelete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        deleteAction.execute(request, listener);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return deleteByQueryAction.submit(request);
    }

    @Override public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        return deleteByQueryAction.submit(request, listener);
    }

    @Override public void execDeleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        deleteByQueryAction.execute(request, listener);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request) {
        return getAction.submit(request);
    }

    @Override public ActionFuture<GetResponse> get(GetRequest request, ActionListener<GetResponse> listener) {
        return getAction.submit(request, listener);
    }

    @Override public void execGet(GetRequest request, ActionListener<GetResponse> listener) {
        getAction.execute(request, listener);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request) {
        return countAction.submit(request);
    }

    @Override public ActionFuture<CountResponse> count(CountRequest request, ActionListener<CountResponse> listener) {
        return countAction.submit(request, listener);
    }

    @Override public void execCount(CountRequest request, ActionListener<CountResponse> listener) {
        countAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request) {
        return searchAction.submit(request);
    }

    @Override public ActionFuture<SearchResponse> search(SearchRequest request, ActionListener<SearchResponse> listener) {
        return searchAction.submit(request, listener);
    }

    @Override public void execSearch(SearchRequest request, ActionListener<SearchResponse> listener) {
        searchAction.execute(request, listener);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return searchScrollAction.submit(request);
    }

    @Override public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        return searchScrollAction.submit(request, listener);
    }

    @Override public void execSearchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        searchScrollAction.execute(request, listener);
    }
}
