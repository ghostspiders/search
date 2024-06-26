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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.server.search.index.Index;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.settings.Settings;

import java.util.Iterator;
import java.util.Set;


public class StandardAnalyzerProvider extends AbstractAnalyzerProvider<StandardAnalyzer> {

    private final Set<String> stopWords;

    private final int maxTokenLength;

    private final StandardAnalyzer standardAnalyzer;

    @Inject public StandardAnalyzerProvider(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        String[] stopWords = settings.getAsArray("stopwords");
        if (stopWords.length > 0) {
            this.stopWords = ImmutableSet.copyOf(Iterators.forArray(stopWords));
        } else {
//            EnglishAnalyzer analyzer = new EnglishAnalyzer();
//            CharArraySet egStopWords = analyzer.getStopwordSet();
//            String[] objects = (String[]) egStopWords.toArray();
            this.stopWords = ImmutableSet.copyOf(new String[]{});
        }
        maxTokenLength = settings.getAsInt("maxTokenLength", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
        standardAnalyzer = new StandardAnalyzer(new CharArraySet(this.stopWords,true));
        standardAnalyzer.setMaxTokenLength(maxTokenLength);
    }

    @Override public StandardAnalyzer get() {
        return this.standardAnalyzer;
    }
}
