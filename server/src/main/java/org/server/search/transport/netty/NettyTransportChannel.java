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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundBuffer;
import org.server.search.transport.NotSerializableTransportException;
import org.server.search.transport.RemoteTransportException;
import org.server.search.transport.TransportChannel;
import org.server.search.util.io.ByteArrayDataOutputStream;
import org.server.search.util.io.Streamable;
import org.server.search.util.io.ThrowableObjectOutputStream;

import java.io.IOException;
import java.io.NotSerializableException;

import static org.server.search.transport.Transport.Helper.*;

/**
 * @author kimchy (Shay Banon)
 */
public class NettyTransportChannel implements TransportChannel {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    private final NettyTransport transport;

    private final String action;

    private final Channel channel;

    private final long requestId;

    public NettyTransportChannel(NettyTransport transport, String action, Channel channel, long requestId) {
        this.transport = transport;
        this.action = action;
        this.channel = channel;
        this.requestId = requestId;
    }

    @Override public String action() {
        return this.action;
    }

    @Override public void sendResponse(Streamable message) throws IOException {
        ByteArrayDataOutputStream stream = ByteArrayDataOutputStream.Cached.cached();
        stream.write(LENGTH_PLACEHOLDER); // fake size
        stream.writeLong(requestId);
        byte status = 0;
        status = setResponse(status);
        stream.writeByte(status); // 0 for request, 1 for response.
        message.writeTo(stream);
        ByteBuf buffer = Unpooled.wrappedBuffer(stream.copiedByteArray());
        buffer.setInt(0, buffer.writerIndex() - 4); // update real size.
        channel.write(buffer);
    }

    @Override public void sendResponse(Throwable error) throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeBytes(LENGTH_PLACEHOLDER);
        buffer.writeLong(requestId);

        byte status = 0;
        status = setResponse(status);
        status = setError(status);
        buffer.writeByte(status);

        buffer.markWriterIndex();
        RemoteTransportException tx = new RemoteTransportException(transport.settings().get("name"), transport.wrapAddress(channel.localAddress()), action, error);
        buffer.writeBytes(tx.getDetailedMessage().getBytes());

        buffer.setInt(0, buffer.writerIndex() - 4);
        channel.write(buffer);
    }
}
