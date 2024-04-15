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

import org.apache.lucene.util.UnicodeUtil;

/**
 * <p/>
 * <p>Note, this class assumes that the utf8 result is not thread safe.
 *
 * @author kimchy (Shay Banon)
 */
public class Utf8HttpResponse extends AbstractHttpResponse implements HttpResponse {

    private final Status status;

    private final String utf8Result;

    private final String prefixUtf8Result;

    private final String suffixUtf8Result;

    public Utf8HttpResponse(Status status) {
        this(status, null);
    }

    public Utf8HttpResponse(Status status, String utf8Result) {
        this(status, utf8Result, null, null);
    }

    public Utf8HttpResponse(Status status, String utf8Result,
                            String prefixUtf8Result, String suffixUtf8Result) {
        this.status = status;
        this.utf8Result = utf8Result;
        this.prefixUtf8Result = prefixUtf8Result;
        this.suffixUtf8Result = suffixUtf8Result;
    }

    @Override public boolean contentThreadSafe() {
        return false;
    }

    @Override public String contentType() {
        return "text/plain; charset=UTF-8";
    }

    @Override public byte[] content() {
        return utf8Result.getBytes();
    }

    @Override public int contentLength() {
        return utf8Result.length();
    }

    @Override public Status status() {
        return status;
    }

    @Override public byte[] prefixContent() {
        return prefixUtf8Result != null ? prefixUtf8Result.getBytes() : null;
    }

    @Override public int prefixContentLength() {
        return prefixUtf8Result != null ? prefixUtf8Result.length() : -1;
    }

    @Override public byte[] suffixContent() {
        return suffixUtf8Result != null ? suffixUtf8Result.getBytes() : null;
    }

    @Override public int suffixContentLength() {
        return suffixUtf8Result != null ? suffixUtf8Result.length() : -1;
    }
}
