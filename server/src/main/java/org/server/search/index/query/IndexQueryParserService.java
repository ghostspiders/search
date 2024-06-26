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

package org.server.search.index.query;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.server.search.index.AbstractIndexComponent;
import org.server.search.index.Index;
import org.server.search.index.analysis.AnalysisService;
import org.server.search.index.mapper.MapperService;
import org.server.search.index.query.json.JsonIndexQueryParser;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.Nullable;
import org.server.search.util.settings.ImmutableSettings;
import org.server.search.util.settings.Settings;

import java.util.Map;

import static com.google.common.collect.Maps.*;

 
public class IndexQueryParserService extends AbstractIndexComponent {

    public static final class Defaults {
        public static final String DEFAULT = "default";
        public static final String PREFIX = "index.queryparser.types";
    }

    private final IndexQueryParser defaultIndexQueryParser;

    private final Map<String, IndexQueryParser> indexQueryParsers;

    public IndexQueryParserService(Index index, MapperService mapperService, AnalysisService analysisService) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS, mapperService, analysisService, null);
    }

    @Inject public IndexQueryParserService(Index index, @IndexSettings Settings indexSettings,
                                           MapperService mapperService,
                                           AnalysisService analysisService,
                                           @Nullable Map<String, IndexQueryParserFactory> indexQueryParsersFactories) {
        super(index, indexSettings);
        Map<String, Settings> queryParserGroupSettings;
        if (indexSettings != null) {
            queryParserGroupSettings = indexSettings.getGroups(Defaults.PREFIX);
        } else {
            queryParserGroupSettings = newHashMap();
        }
        Map<String, IndexQueryParser> qparsers = newHashMap();
        if (indexQueryParsersFactories != null) {
            for (Map.Entry<String, IndexQueryParserFactory> entry : indexQueryParsersFactories.entrySet()) {
                String qparserName = entry.getKey();
                Settings qparserSettings = queryParserGroupSettings.get(qparserName);
                qparsers.put(qparserName, entry.getValue().create(qparserName, qparserSettings));
            }
        }
        if (!qparsers.containsKey(Defaults.DEFAULT)) {
            IndexQueryParser defaultQueryParser = new JsonIndexQueryParser(index, indexSettings, mapperService, analysisService, null, null, null);
            qparsers.put(Defaults.DEFAULT, defaultQueryParser);
        }

        indexQueryParsers = ImmutableMap.copyOf(qparsers);

        defaultIndexQueryParser = indexQueryParser(Defaults.DEFAULT);
    }

    public IndexQueryParser indexQueryParser(String name) {
        return indexQueryParsers.get(name);
    }

    public IndexQueryParser defaultIndexQueryParser() {
        return defaultIndexQueryParser;
    }
}
