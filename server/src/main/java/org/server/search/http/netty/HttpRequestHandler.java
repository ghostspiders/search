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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;


// 使用@ChannelHandler.Sharable注解，表示这个处理器可以在多个Channel之间共享
@ChannelHandler.Sharable
public class HttpRequestHandler extends ChannelInboundHandlerAdapter {

    // NettyHttpServerTransport的实例，用于将请求分派到相应的处理器
    private final NettyHttpServerTransport serverTransport;

    // 构造函数，接收NettyHttpServerTransport的实例
    public HttpRequestHandler(NettyHttpServerTransport serverTransport) {
        this.serverTransport = serverTransport;
    }

    // 当Channel读取到消息时调用的方法
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // 检查消息是否是FullHttpRequest的实例
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest request = (FullHttpRequest) msg; // 转换消息为FullHttpRequest
            // 使用serverTransport分派请求到相应的处理器
            serverTransport.dispatchRequest(
                    new NettyHttpRequest(request), // 创建NettyHttpRequest包装器
                    new NettyHttpChannel(ctx.channel(), request) // 创建NettyHttpChannel包装器
            );
        }
    }

    // 当捕获到异常时调用的方法
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 调用serverTransport的exceptionCaught方法来处理异常
        serverTransport.exceptionCaught(ctx, cause);
    }
}