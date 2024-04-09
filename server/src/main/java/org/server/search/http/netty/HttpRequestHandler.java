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

import com.sun.jdi.event.ExceptionEvent;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;

/**
 * @author kimchy (Shay Banon)
 */
@ChannelHandler.Sharable
public class HttpRequestHandler extends ChannelInboundHandlerAdapter {

    private final NettyHttpServerTransport serverTransport;

    public HttpRequestHandler(NettyHttpServerTransport serverTransport) {
        this.serverTransport = serverTransport;
    }

    public void messageReceived(ChannelHandlerContext ctx, Object msg) throws Exception {
        HttpRequest request = (HttpRequest) msg.getMessage();
        serverTransport.dispatchRequest(new NettyHttpRequest(request), new NettyHttpChannel(e.getChannel(), request));
        super.messageReceived(ctx, msg);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        serverTransport.exceptionCaught(ctx, cause);
    }
}
