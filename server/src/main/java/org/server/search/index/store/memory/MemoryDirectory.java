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

package org.server.search.index.store.memory;

import org.apache.lucene.store.*;
import org.server.search.util.SizeUnit;
import org.server.search.util.SizeValue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static org.server.search.util.concurrent.ConcurrentMaps.*;

/**
 * @author kimchy (Shay Banon)
 */
public class MemoryDirectory extends Directory {

    private final Map<String, MemoryFile> files = newConcurrentMap();

    private final Queue<byte[]> cache;

    private final int bufferSizeInBytes;

    private final SizeValue bufferSize;

    private final SizeValue cacheSize;

    private final boolean disableCache;

    public MemoryDirectory() {
        this(new SizeValue(1, SizeUnit.KB), new SizeValue(20, SizeUnit.MB), false);
    }

    public MemoryDirectory(SizeValue bufferSize, SizeValue cacheSize, boolean warmCache) {
        disableCache = cacheSize.bytes() == 0;
        if (!disableCache && cacheSize.bytes() < bufferSize.bytes()) {
            throw new IllegalArgumentException("Cache size [" + cacheSize + "] is smaller than buffer size [" + bufferSize + "]");
        }
        this.bufferSize = bufferSize;
        this.bufferSizeInBytes = (int) bufferSize.bytes();
        int numberOfCacheEntries = (int) (cacheSize.bytes() / bufferSize.bytes());
        this.cache = disableCache ? null : new ArrayBlockingQueue<byte[]>(numberOfCacheEntries);
        this.cacheSize = disableCache ? new SizeValue(0, SizeUnit.BYTES) : new SizeValue(numberOfCacheEntries * bufferSize.bytes(), SizeUnit.BYTES);
        if (!disableCache && warmCache) {
            for (int i = 0; i < numberOfCacheEntries; i++) {
                cache.add(createBuffer());
            }
        }
    }

    public SizeValue bufferSize() {
        return this.bufferSize;
    }

    public SizeValue cacheSize() {
        return this.cacheSize;
    }

    int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    @Override public String[] listAll() throws IOException {
        return files.keySet().toArray(new String[0]);
    }

    @Override public void deleteFile(String name) throws IOException {
        MemoryFile file = files.remove(name);
        if (file == null)
            throw new FileNotFoundException(name);
        file.clean();
    }

    @Override public long fileLength(String name) throws IOException {
        MemoryFile file = files.get(name);
        if (file == null)
            throw new FileNotFoundException(name);
        return file.length();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return null;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return null;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }


    @Override
    public void syncMetaData() throws IOException {

    }


    @Override
    public void rename(String source, String dest) throws IOException {

    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return null;
    }

    @Override
    public Lock obtainLock(String name) throws IOException {
        return null;
    }

    @Override public void close() throws IOException {
        String[] files = listAll();
        for (String file : files) {
            deleteFile(file);
        }
        if (!disableCache) {
            byte[] buffer = cache.poll();
            while (buffer != null) {
                closeBuffer(buffer);
                buffer = cache.poll();
            }
        }
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return null;
    }

    void releaseBuffer(byte[] buffer) {
        if (disableCache) {
            closeBuffer(buffer);
            return;
        }
        boolean success = cache.offer(buffer);
        if (!success) {
            closeBuffer(buffer);
        }
    }

    byte[] acquireBuffer() {
        if (disableCache) {
            return createBuffer();
        }
        byte[] buffer = cache.poll();
        if (buffer == null) {
            // everything is taken, return a new one
            return createBuffer();
        }
        return buffer;
    }

    byte[] createBuffer() {
        return new byte[bufferSizeInBytes];
    }

    void closeBuffer(byte[] buffer) {
    }
}