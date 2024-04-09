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

package org.server.search.http.action.admin.indices.delete;

import com.google.inject.Inject;
import org.server.search.ExceptionsHelper;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.indices.delete.DeleteIndexRequest;
import org.server.search.action.admin.indices.delete.DeleteIndexResponse;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.indices.IndexMissingException;
import org.server.search.util.TimeValue;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

import static org.server.search.http.HttpResponse.Status.*;
import static org.server.search.util.TimeValue.*;
import static org.server.search.util.json.JsonBuilder.Cached.*;

/**
 * @author kimchy (Shay Banon)
 */
public class HttpDeleteIndexAction extends BaseHttpServerHandler {

    @Inject public HttpDeleteIndexAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.DELETE, "/{index}", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(request.param("index"));
        deleteIndexRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), timeValueSeconds(10)));
        client.admin().indices().execDelete(deleteIndexRequest, new ActionListener<DeleteIndexResponse>() {
            @Override public void onResponse(DeleteIndexResponse result) {
                try {
                    channel.sendResponse(new JsonHttpResponse(request, OK, cached().startObject().field("ok", true).endObject()));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexMissingException) {
                        JsonBuilder builder = HttpJsonBuilder.cached(request);
                        builder.startObject()
                                .field("ok", true)
                                .endObject();
                        channel.sendResponse(new JsonHttpResponse(request, NOT_FOUND, builder));
                    } else {
                        channel.sendResponse(new JsonThrowableHttpResponse(request, e));
                    }
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }
}