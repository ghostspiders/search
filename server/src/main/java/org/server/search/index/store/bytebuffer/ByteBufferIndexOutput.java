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

package org.server.search.index.store.bytebuffer;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

 
public class ByteBufferIndexOutput extends IndexOutput {

    private final ByteBuffersDirectory dir;
    private final ByteBufferFile file;

    private ByteBuffer currentBuffer;
    private int currentBufferIndex;

    private long bufferStart;
    private int bufferLength;

    private ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

    public ByteBufferIndexOutput(ByteBuffersDirectory dir, ByteBufferFile file) throws IOException {
        super("","");
        this.dir = dir;
        this.file = file;
        switchCurrentBuffer();
    }

    @Override public void writeByte(byte b) throws IOException {
        if (!currentBuffer.hasRemaining()) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        currentBuffer.put(b);
    }

    @Override public void writeBytes(byte[] b, int offset, int len) throws IOException {
        while (len > 0) {
            if (!currentBuffer.hasRemaining()) {
                currentBufferIndex++;
                switchCurrentBuffer();
            }

            int remainInBuffer = currentBuffer.remaining();
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            currentBuffer.put(b, offset, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
        }
    }


    @Override public void close() throws IOException {
        file.buffers(buffers.toArray(new ByteBuffer[buffers.size()]));
    }

    @Override public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + currentBuffer.position();
    }

    /**
     * Returns the current checksum of bytes written so far
     */
    @Override
    public long getChecksum() throws IOException {
        return 0;
    }

    private void switchCurrentBuffer() throws IOException {

    }

    private void setFileLength() {
        long pointer = bufferStart + currentBuffer.position();
        if (pointer > file.length()) {
            file.length(pointer);
        }
    }
}
