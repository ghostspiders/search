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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.google.inject.Inject;
import org.apache.lucene.search.Query;
import org.server.search.SearchException;
import org.server.search.index.AbstractIndexComponent;
import org.server.search.index.Index;
import org.server.search.index.analysis.AnalysisService;
import org.server.search.index.mapper.MapperService;
import org.server.search.index.query.IndexQueryParser;
import org.server.search.index.query.QueryBuilder;
import org.server.search.index.query.QueryParsingException;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.Nullable;
import org.server.search.util.io.FastStringReader;
import org.server.search.util.json.Jackson;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;

 
public class JsonIndexQueryParser extends AbstractIndexComponent implements IndexQueryParser {

    public static final class Defaults {
        public static final String JSON_QUERY_PREFIX = "index.queryparser.json.query";
        public static final String JSON_FILTER_PREFIX = "index.queryparser.json.filter";
    }

    private ThreadLocal<JsonQueryParseContext> cache = new ThreadLocal<JsonQueryParseContext>() {
        @Override protected JsonQueryParseContext initialValue() {
            return new JsonQueryParseContext(index, queryParserRegistry, mapperService);
        }
    };

    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    private final String name;

    private final MapperService mapperService;

    private final JsonQueryParserRegistry queryParserRegistry;

    @Inject public JsonIndexQueryParser(Index index,
                                        @IndexSettings Settings indexSettings,
                                        MapperService mapperService,
                                        AnalysisService analysisService,
                                        @Nullable Map<String, JsonQueryParserFactory> jsonQueryParsers,
                                        String name, @Nullable Settings settings) {
        super(index, indexSettings);
        this.name = name;
        this.mapperService = mapperService;

        List<JsonQueryParser> queryParsers = newArrayList();
        if (jsonQueryParsers != null) {
            Map<String, Settings> jsonQueryParserGroups = indexSettings.getGroups(Defaults.JSON_QUERY_PREFIX);
            for (Map.Entry<String, JsonQueryParserFactory> entry : jsonQueryParsers.entrySet()) {
                String queryParserName = entry.getKey();
                JsonQueryParserFactory queryParserFactory = entry.getValue();
                Settings queryParserSettings = jsonQueryParserGroups.get(queryParserName);

                queryParsers.add(queryParserFactory.create(queryParserName, queryParserSettings));
            }
        }

        this.queryParserRegistry = new JsonQueryParserRegistry(index, indexSettings, analysisService, queryParsers);
    }

    @Override public String name() {
        return this.name;
    }

    public JsonQueryParserRegistry queryParserRegistry() {
        return this.queryParserRegistry;
    }

    @Override public Query parse(QueryBuilder queryBuilder) throws SearchException {
        return parse(queryBuilder.build());
    }

    @Override public Query parse(String source) throws QueryParsingException {
        try {
            return parse(cache.get(), source, jsonFactory.createJsonParser(new FastStringReader(source)));
        } catch (QueryParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryParsingException(index, "Failed to parse [" + source + "]", e);
        }
    }

    public Query parse(JsonParser jsonParser, String source) {
        try {
            return parse(cache.get(), source, jsonParser);
        } catch (IOException e) {
            throw new QueryParsingException(index, "Failed to parse [" + source + "]", e);
        }
    }

    private Query parse(JsonQueryParseContext parseContext, String source, JsonParser jsonParser) throws IOException, QueryParsingException {
        parseContext.reset(jsonParser);
        return parseContext.parseInnerQuery();
    }
}
