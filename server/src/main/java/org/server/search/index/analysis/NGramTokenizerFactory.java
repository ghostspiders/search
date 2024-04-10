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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.server.search.index.Index;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.settings.Settings;

import java.io.Reader;

/**
 * @author kimchy (Shay Banon)
 */
public class NGramTokenizerFactory extends AbstractTokenizerFactory {

    private final int minGram;

    private final int maxGram;

    @Inject public NGramTokenizerFactory(Index index, @IndexSettings Settings indexSettings, String name, Settings settings) {
        super(index, indexSettings, name);
        this.minGram = settings.getAsInt("minGram", NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
        this.maxGram = settings.getAsInt("maxGram", NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
    }

    @Override public Tokenizer create(Reader reader) {
        return new NGramTokenizer(reader, minGram, maxGram);
    }
}