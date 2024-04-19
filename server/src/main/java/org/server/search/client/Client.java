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

package org.server.search.client;

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

/**
 * @author kimchy (Shay Banon)
 */
public interface Client {

    void close() throws InterruptedException;

    AdminClient admin();

    ActionFuture<IndexResponse> index(IndexRequest request);

    ActionFuture<IndexResponse> index(IndexRequest request, ActionListener<IndexResponse> listener);

    void execIndex(IndexRequest request, ActionListener<IndexResponse> listener);

    ActionFuture<DeleteResponse> delete(DeleteRequest request);

    ActionFuture<DeleteResponse> delete(DeleteRequest request, ActionListener<DeleteResponse> listener);

    void execDelete(DeleteRequest request, ActionListener<DeleteResponse> listener);

    ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request);

    ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener);

    void execDeleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener);

    ActionFuture<GetResponse> get(GetRequest request);

    ActionFuture<GetResponse> get(GetRequest request, ActionListener<GetResponse> listener);

    void execGet(GetRequest request, ActionListener<GetResponse> listener);

    ActionFuture<CountResponse> count(CountRequest request);

    ActionFuture<CountResponse> count(CountRequest request, ActionListener<CountResponse> listener);

    void execCount(CountRequest request, ActionListener<CountResponse> listener);

    ActionFuture<SearchResponse> search(SearchRequest request);

    ActionFuture<SearchResponse> search(SearchRequest request, ActionListener<SearchResponse> listener);

    void execSearch(SearchRequest request, ActionListener<SearchResponse> listener);

    ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request);

    ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener);

    void execSearchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener);
}