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

package org.server.search.search.fetch;

import org.server.search.SearchException;
import org.server.search.search.internal.SearchContext;

 
public class FetchPhaseExecutionException extends SearchException {

    public FetchPhaseExecutionException(SearchContext context, String msg) {
        this(context, msg, null);
    }

    public FetchPhaseExecutionException(SearchContext context, String msg, Throwable t) {
        super("Failed to fetch query [" + context.query() + "], sort [" + context.sort() + "], from [" + context.from() + "], size [" + context.size() + "], reason [" + msg + "]", t);
    }
}
