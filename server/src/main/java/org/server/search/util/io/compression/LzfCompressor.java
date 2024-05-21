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

package org.server.search.util.io.compression;

import org.apache.lucene.util.UnicodeUtil;
import org.server.search.util.io.compression.lzf.LZFDecoder;
import org.server.search.util.io.compression.lzf.LZFEncoder;

import java.io.IOException;

 
public class LzfCompressor implements Compressor {

    private static class Cached {

        private static final ThreadLocal<CompressHolder> cache = new ThreadLocal<CompressHolder>() {
            @Override protected CompressHolder initialValue() {
                return new CompressHolder();
            }
        };

        public static CompressHolder cached() {
            return cache.get();
        }
    }

    private static class CompressHolder {
        final String utf16Result = new String();
        final String utf8Result = new String();
    }

    @Override public byte[] compress(byte[] value) throws IOException {
        return LZFEncoder.encode(value, value.length);
    }

    @Override public byte[] compressString(String value) throws IOException {
        CompressHolder ch = Cached.cached();
        return LZFEncoder.encode(ch.utf8Result.getBytes(), ch.utf8Result.length());
    }

    @Override public byte[] decompress(byte[] value) throws IOException {
        return LZFDecoder.decode(value, value.length);
    }

    @Override public String decompressString(byte[] value) throws IOException {
        CompressHolder ch = Cached.cached();
        byte[] result = decompress(value);
        return new String(ch.utf16Result.getBytes(), 0, ch.utf16Result.length());
    }
}
