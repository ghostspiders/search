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

package org.server.search.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.*;
import org.server.search.util.io.DataInputInputStream;
import org.server.search.util.io.Streamable;
import org.server.search.util.io.ThrowableObjectInputStream;
import io.netty.channel.*;
import org.slf4j.Logger;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.server.search.transport.Transport.Helper.*;

 
public class MessageChannelHandler extends ChannelInboundHandlerAdapter  {

    private final Logger logger;

    private final ThreadPool threadPool;

    private final TransportServiceAdapter transportServiceAdapter;

    private final NettyTransport transport;

    public MessageChannelHandler(NettyTransport transport, Logger logger) {
        this.threadPool = transport.threadPool();
        this.transportServiceAdapter = transport.transportServiceAdapter();
        this.transport = transport;
        this.logger = logger;
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf) msg;

        long requestId = buffer.readLong();
        byte status = buffer.readByte();
        boolean isRequest = isRequest(status);

        if (isRequest) {
            handleRequest(ctx, buffer, requestId);
        } else {
            final TransportResponseHandler handler = transportServiceAdapter.remove(requestId);
            if (handler == null) {
                throw new ResponseHandlerNotFoundTransportException(requestId);
            }
            if (isError(status)) {
                handlerResponseError(buffer, handler);
            } else {
                handleResponse(buffer, handler);
            }
        }
    }

    private void handleResponse(ByteBuf buffer, final TransportResponseHandler handler) {
        final Streamable streamable = handler.newInstance();
        try {
            InputStream inputStream = new ByteBufInputStream(buffer);
            streamable.readFrom(new DataInputStream(inputStream));
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException("Failed to deserialize response of type [" + streamable.getClass().getName() + "]", e));
            return;
        }
        if (handler.spawn()) {
            threadPool.execute(new Runnable() {
                @SuppressWarnings({"unchecked"}) @Override public void run() {
                    try {
                        handler.handleResponse(streamable);
                    } catch (Exception e) {
                        handleException(handler, new ResponseHandlerFailureTransportException("Failed to handler response", e));
                    }
                }
            });
        } else {
            try {
                //noinspection unchecked
                handler.handleResponse(streamable);
            } catch (Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException("Failed to handler response", e));
            }
        }
    }

    private void handlerResponseError(ByteBuf buffer , final TransportResponseHandler handler) {
        Throwable error;
        try {
            InputStream inputStream = new ByteBufInputStream(buffer);
            ThrowableObjectInputStream ois = new ThrowableObjectInputStream(new DataInputStream(inputStream));
            error = (Throwable) ois.readObject();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException("None remote transport exception", error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        if (handler.spawn()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        handler.handleException(rtx);
                    } catch (Exception e) {
                        logger.error("Failed to handle exception response", e);
                    }
                }
            });
        } else {
            handler.handleException(rtx);
        }
    }

    private void handleRequest(ChannelHandlerContext ctx, ByteBuf buffer, long requestId) throws IOException {
        final String action = buffer.toString();

        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, action, ctx.channel(), requestId);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action);
            if (handler == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final Streamable streamable = handler.newInstance();

            InputStream inputStream = new ByteBufInputStream(buffer);
            streamable.readFrom(new DataInputStream(inputStream));
            if (handler.spawn()) {
                threadPool.execute(new Runnable() {
                    @SuppressWarnings({"unchecked"}) @Override public void run() {
                        try {
                            handler.messageReceived(streamable, transportChannel);
                        } catch (Throwable e) {
                            try {
                                transportChannel.sendResponse(e);
                            } catch (IOException e1) {
                                logger.warn("Failed to send error message back to client for action [" + action + "]", e1);
                                logger.warn("Actual Exception", e);
                            }
                        }
                    }
                });
            } else {
                //noinspection unchecked
                handler.messageReceived(streamable, transportChannel);
            }
        } catch (Exception e) {
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                logger.warn("Actual Exception", e1);
            }
        }
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        transport.exceptionCaught(ctx, cause);
    }
}
