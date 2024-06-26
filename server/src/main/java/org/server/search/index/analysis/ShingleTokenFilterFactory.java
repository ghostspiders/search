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

package org.server.search.index.analysis;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.server.search.index.Index;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.settings.Settings;

 
public class ShingleTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int maxShingleSize;

    private final boolean outputUnigrams;

    @Inject public ShingleTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        maxShingleSize = settings.getAsInt("maxShingleSize", ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
        outputUnigrams = settings.getAsBoolean("outputUnigrams", true);
    }

    @Override public TokenStream create(TokenStream tokenStream) {
        ShingleFilter filter = new ShingleFilter(tokenStream, maxShingleSize);
        filter.setOutputUnigrams(outputUnigrams);
        return filter;
    }
}