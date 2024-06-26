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

package org.server.search.util;

/**
 * A <tt>SizeUnit</tt> represents size at a given unit of
 * granularity and provides utility methods to convert across units.
 * A <tt>SizeUnit</tt> does not maintain size information, but only
 * helps organize and use size representations that may be maintained
 * separately across various contexts.
 *
 * 
 */
public enum SizeUnit {
    BYTES {
        @Override public long toBytes(long size) {
            return size;
        }@Override public long toKB(long size) {
            return size / (C1 / C0);
        }@Override public long toMB(long size) {
            return size / (C2 / C0);
        }@Override public long toGB(long size) {
            return size / (C3 / C0);
        }},
    KB {
        @Override public long toBytes(long size) {
            return x(size, C1 / C0, MAX / (C1 / C0));
        }@Override public long toKB(long size) {
            return size;
        }@Override public long toMB(long size) {
            return size / (C2 / C1);
        }@Override public long toGB(long size) {
            return size / (C3 / C1);
        }},
    MB {
        @Override public long toBytes(long size) {
            return x(size, C2 / C0, MAX / (C2 / C0));
        }@Override public long toKB(long size) {
            return x(size, C2 / C1, MAX / (C2 / C1));
        }@Override public long toMB(long size) {
            return size;
        }@Override public long toGB(long size) {
            return size / (C3 / C2);
        }},
    GB {
        @Override public long toBytes(long size) {
            return x(size, C3 / C0, MAX / (C3 / C0));
        }@Override public long toKB(long size) {
            return x(size, C3 / C1, MAX / (C3 / C1));
        }@Override public long toMB(long size) {
            return x(size, C3 / C2, MAX / (C3 / C2));
        }@Override public long toGB(long size) {
            return size;
        }};

    static final long C0 = 1L;
    static final long C1 = C0 * 1024L;
    static final long C2 = C1 * 1024L;
    static final long C3 = C2 * 1024L;

    static final long MAX = Long.MAX_VALUE;

    /**
     * Scale d by m, checking for overflow.
     * This has a short name to make above code more readable.
     */
    static long x(long d, long m, long over) {
        if (d > over) return Long.MAX_VALUE;
        if (d < -over) return Long.MIN_VALUE;
        return d * m;
    }


    public long toBytes(long size) {
        throw new AbstractMethodError();
    }

    public long toKB(long size) {
        throw new AbstractMethodError();
    }

    public long toMB(long size) {
        throw new AbstractMethodError();
    }

    public long toGB(long size) {
        throw new AbstractMethodError();
    }
}
