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

package org.server.search.http.action.main;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.server.search.Version;
import org.server.search.client.Client;
import org.server.search.http.*;
import org.server.search.http.action.support.HttpJsonBuilder;
import org.server.search.util.Classes;
import org.server.search.util.concurrent.ThreadLocalRandom;
import org.server.search.util.json.Jackson;
import org.server.search.util.json.JsonBuilder;
import org.server.search.util.settings.Settings;

import java.io.IOException;

 
public class HttpMainAction extends BaseHttpServerHandler {

    private final JsonNode rootNode;

    private final int quotesSize;

    @Inject public HttpMainAction(Settings settings, HttpServer httpServer, Client client) {
        super(settings, client);
        JsonNode rootNode;
        int quotesSize;
        try {
            rootNode = Jackson.newObjectMapper().readValue(Classes.getDefaultClassLoader().getResourceAsStream("org/server/search/http/action/main/quotes.json"), JsonNode.class);
            ArrayNode arrayNode = (ArrayNode) rootNode.get("quotes");
            quotesSize = Iterators.size(arrayNode.elements());
        } catch (Exception e) {
            rootNode = null;
            quotesSize = -1;
        }
        this.rootNode = rootNode;
        this.quotesSize = quotesSize;

        httpServer.registerHandler(HttpRequest.Method.GET, "/", this);
    }

    @Override public void handleRequest(HttpRequest request, HttpChannel channel) {
        try {
            JsonBuilder builder = HttpJsonBuilder.cached(request).prettyPrint();
            builder.startObject();
            builder.field("ok", true);
            if (settings.get("name") != null) {
                builder.field("name", settings.get("name"));
            }
            builder.startObject("version").field("number", Version.number()).field("date", Version.date()).field("devBuild", Version.devBuild()).endObject();
            builder.field("version", Version.number());
            builder.field("cover", "DON'T PANIC");
            if (rootNode != null) {
                builder.startObject("quote");
                ArrayNode arrayNode = (ArrayNode) rootNode.get("quotes");
                JsonNode quoteNode = arrayNode.get(ThreadLocalRandom.current().nextInt(quotesSize));
                builder.field("book", quoteNode.get("book").textValue());
                builder.field("chapter", quoteNode.get("chapter").textValue());
                ArrayNode textNodes = (ArrayNode) quoteNode.get("text");
                int index = 0;
                for (JsonNode textNode : textNodes) {
                    builder.field("text" + (++index), textNode.textValue());
                }
                builder.endObject();
            }
            builder.endObject();
            channel.sendResponse(new JsonHttpResponse(request, HttpResponse.Status.OK, builder));
        } catch (Exception e) {
            try {
                channel.sendResponse(new JsonThrowableHttpResponse(request, e));
            } catch (IOException e1) {
                logger.warn("Failed to send response", e);
            }
        }
    }
}
