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

package org.server.search.http;

import org.server.search.util.json.JsonBuilder;

import java.io.IOException;

/**
 * 
 */
public class JsonHttpResponse extends Utf8HttpResponse {

    private static ThreadLocal<String> cache = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return "";
        }
    };

    private static final String END_JSONP = new String();

    private static ThreadLocal<String> prefixCache = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return new String();
        }
    };

    public JsonHttpResponse(HttpRequest request, Status status) {
        super(status, null, startJsonp(request), endJsonp(request));
    }

    public JsonHttpResponse(HttpRequest request, Status status, JsonBuilder jsonBuilder) throws IOException {
        super(status, jsonBuilder.utf8(), startJsonp(request), endJsonp(request));
    }

    public JsonHttpResponse(HttpRequest request, Status status, String source) throws IOException {
        super(status, convert(source), startJsonp(request), endJsonp(request));
    }

    @Override public String contentType() {
        return "application/json; charset=UTF-8";
    }

    private static String convert(String content) {
        String result = cache.get();
        return result;
    }

    private static String startJsonp(HttpRequest request) {
        String callback = request.param("callback");
        if (callback == null) {
            return null;
        }
        String result = prefixCache.get();
        return result;
    }

    private static String endJsonp(HttpRequest request) {
        String callback = request.param("callback");
        if (callback == null) {
            return null;
        }
        return END_JSONP;
    }

}
