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

package org.server.search.http.action.admin.indices.create;

import com.google.inject.Inject;
import org.server.search.action.ActionListener;
import org.server.search.action.admin.indices.create.CreateIndexRequest;
import org.server.search.action.admin.indices.create.CreateIndexResponse;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.indices.IndexAlreadyExistsException;
import org.server.search.indices.InvalidIndexNameException;
import org.server.search.util.Strings;
import org.server.search.util.TimeValue;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.ImmutableSettings;
import org.server.search.util.settings.Settings;
import org.server.search.util.settings.SettingsException;

import java.io.IOException;

import static org.server.search.ExceptionsHelper.*;
import static org.server.search.http.HttpResponse.Status.*;
import static org.server.search.util.TimeValue.*;

/**
 * 
 */
public class HttpCreateIndexAction extends BaseHttpServerHandler {

    @Inject public HttpCreateIndexAction(Settings settings, HttpServer httpService, Client client) {
        super(settings, client);
        httpService.registerHandler(HttpRequest.Method.PUT, "/{index}", this);
    }

    @Override public void handleRequest(final HttpRequest request, final HttpChannel channel) {
        String bodySettings = request.contentAsString();
        Settings indexSettings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        if (Strings.hasText(bodySettings)) {
            try {
                indexSettings = ImmutableSettings.settingsBuilder().loadFromSource(bodySettings).build();
            } catch (Exception e) {
                try {
                    channel.sendResponse(new JsonThrowableHttpResponse(request, BAD_REQUEST, new SettingsException("Failed to parse index settings", e)));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"), indexSettings);
        createIndexRequest.timeout(TimeValue.parseTimeValue(request.param("timeout"), timeValueSeconds(10)));
        client.admin().indices().execCreate(createIndexRequest, new ActionListener<CreateIndexResponse>() {
            @Override public void onResponse(CreateIndexResponse result) {
                try {
                    JsonBuilder builder = HttpJsonBuilder.cached(request);
                    builder.startObject()
                            .field("ok", true)
                            .endObject();
                    channel.sendResponse(new JsonHttpResponse(request, OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override public void onFailure(Throwable e) {
                try {
                    Throwable t = unwrapCause(e);
                    if (t instanceof IndexAlreadyExistsException || t instanceof InvalidIndexNameException) {
                        channel.sendResponse(new JsonHttpResponse(request, BAD_REQUEST, JsonBuilder.cached().startObject().field("error", t.getMessage()).endObject()));
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
