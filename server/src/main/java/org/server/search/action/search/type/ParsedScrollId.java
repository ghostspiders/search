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

package org.server.search.action.search.type;

import org.server.search.util.Tuple;


public class ParsedScrollId {

    public static String QUERY_THEN_FETCH_TYPE = "queryThenFetch";

    public static String QUERY_AND_FETCH_TYPE = "queryAndFetch";

    private final String source;

    private final String type;

    private final Tuple<String, Long>[] values;

    public ParsedScrollId(String source, String type, Tuple<String, Long>[] values) {
        this.source = source;
        this.type = type;
        this.values = values;
    }

    public String source() {
        return source;
    }

    public String type() {
        return type;
    }

    public Tuple<String, Long>[] values() {
        return values;
    }
}
