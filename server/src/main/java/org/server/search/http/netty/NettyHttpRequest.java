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

package org.server.search.http.netty;

import cn.hutool.core.util.StrUtil;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.server.search.http.HttpRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 */
public class NettyHttpRequest implements HttpRequest {

    private final FullHttpRequest  request;
    private final String content;

    private QueryStringDecoder queryStringDecoder;

    public NettyHttpRequest(FullHttpRequest request) {
        this.request = request;
        this.queryStringDecoder = new QueryStringDecoder(request.uri());
        this.content = getContentFromRequest(request);
    }

    @Override public Method method() {
        HttpMethod httpMethod = request.getMethod();
        if (httpMethod == HttpMethod.GET)
            return Method.GET;

        if (httpMethod == HttpMethod.POST)
            return Method.POST;

        if (httpMethod == HttpMethod.PUT)
            return Method.PUT;

        if (httpMethod == HttpMethod.DELETE)
            return Method.DELETE;

        return Method.GET;
    }

    @Override public String uri() {
        return request.uri();
    }

    @Override public boolean hasContent() {
        return StrUtil.isNotBlank(content);
    }

    @Override public String contentAsString() {
        return content;
    }

    @Override public Set<String> headerNames() {
        return request.headers().names();
    }

    @Override public String header(String name) {
        return request.headers().get(name);
    }

    @Override public List<String> headers(String name) {
        return request.headers().getAll(name);
    }

    @Override public String cookie() {
        return request.headers().get(HttpHeaders.Names.COOKIE);
    }

    @Override public String param(String key) {
        List<String> keyParams = params(key);
        if (keyParams == null || keyParams.isEmpty()) {
            return null;
        }
        return keyParams.get(0);
    }

    @Override public List<String> params(String key) {
        return queryStringDecoder.parameters().get(key);
    }

    @Override public Map<String, List<String>> params() {
        return queryStringDecoder.parameters();
    }
    private String getContentFromRequest(FullHttpRequest request) {
        ByteBuf buffer = request.content();
        try {
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return new String(bytes, CharsetUtil.UTF_8);
        } finally {
            if (buffer != null) {
                buffer.release();
            }
        }
    }
}
