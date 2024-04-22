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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;
import java.io.StreamCorruptedException;
import java.util.List;

/**
 * 
 */
public class SizeHeaderFrameDecoder extends ByteToMessageDecoder {
    private int length;


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {

        if (byteBuf.readableBytes() < 4) {
            return;
        }

        int dataLen = byteBuf.getInt(byteBuf.readerIndex());
        if (dataLen <= 0) {
            throw new StreamCorruptedException("invalid data length: " + dataLen);
        }

        if (byteBuf.readableBytes() < dataLen + 4) {
            return ;
        }

        byteBuf.skipBytes(4);

        length = byteBuf.readInt();
        byte[] content = new byte[length];
        byteBuf.readBytes(content); // 读取消息内容
        out.add(new String(content, CharsetUtil.UTF_8));
        length = 0;
    }
}
