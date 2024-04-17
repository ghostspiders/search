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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;

/**
 * @author kimchy (Shay Banon)
 */
public class CustomAnalyzer extends Analyzer implements PositionIncrementGapAnalyzer {

    private final TokenizerFactory tokenizerFactory;

    private final TokenFilterFactory[] tokenFilters;

    private int positionIncrementGap = 0;

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, TokenFilterFactory[] tokenFilters) {
        this.tokenizerFactory = tokenizerFactory;
        this.tokenFilters = tokenFilters;
    }

    @Override public void setPositionIncrementGap(int positionIncrementGap) {
        this.positionIncrementGap = positionIncrementGap;
    }

    public TokenizerFactory tokenizerFactory() {
        return tokenizerFactory;
    }

    public TokenFilterFactory[] tokenFilters() {
        return tokenFilters;
    }

    /**
     * Creates a new {@link TokenStreamComponents} instance for this analyzer.
     *
     * @param fieldName the name of the fields content passed to the {@link TokenStreamComponents}
     *                  sink as a reader
     * @return the {@link TokenStreamComponents} for this analyzer.
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = this.tokenizerFactory().create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : tokenFilters()) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }
}
