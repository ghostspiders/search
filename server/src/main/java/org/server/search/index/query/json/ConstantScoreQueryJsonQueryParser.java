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

package org.server.search.index.query.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.inject.Inject;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.server.search.index.AbstractIndexComponent;
import org.server.search.index.Index;
import org.server.search.index.query.QueryParsingException;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.settings.Settings;

import java.io.IOException;

 
public class ConstantScoreQueryJsonQueryParser extends AbstractIndexComponent implements JsonQueryParser {

    public static final String NAME = "constantScore";

    @Inject public ConstantScoreQueryJsonQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String name() {
        return NAME;
    }

    @Override public Query parse(JsonQueryParseContext parseContext) throws IOException, QueryParsingException {
        JsonParser jp = parseContext.jp();

        float boost = 1.0f;

        String currentFieldName = null;
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.START_OBJECT) {

            } else if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
                if ("boost".equals(currentFieldName)) {
                    boost = jp.getFloatValue();
                }
            }
        }

        // we don't cache the filter, we assume it is already cached in the filter parsers...
        ConstantScoreQuery query = new ConstantScoreQuery(parseContext.parseInnerQuery());
        return query;
    }
}