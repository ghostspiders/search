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

package org.server.search.index.query.support;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.server.search.index.mapper.FieldMapper;
import org.server.search.index.mapper.FieldMappers;
import org.server.search.index.mapper.MapperService;
import org.server.search.util.Nullable;

import static org.server.search.index.query.support.QueryParsers.*;

/**
 * A query parser that uses the {@link MapperService} in order to build smarter
 * queries based on the mapping information.
 *
 * <p>Maps a logic name of a field {@link FieldMapper#name()}
 * into its {@link FieldMapper#indexName()}.
 *
 * <p>Also breaks fields with [type].[name] into a boolean query that must include the type
 * as well as the query on the name.
 *
 * 
 */
public class MapperQueryParser extends QueryParser {

    private final MapperService mapperService;

    public MapperQueryParser(String defaultField, Analyzer analyzer,
                             @Nullable MapperService mapperService) {
        super(defaultField, analyzer);
        this.mapperService = mapperService;
        setMultiTermRewriteMethod(MultiTermQuery.DOC_VALUES_REWRITE);
    }

    @Override protected Query getPrefixQuery(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    indexedNameField = fieldMappers.fieldMappers().mapper().indexName();
                }
                return wrapSmartNameQuery(super.getPrefixQuery(indexedNameField, termStr), fieldMappers);
            }
        }
        return super.getPrefixQuery(indexedNameField, termStr);
    }

    @Override protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    indexedNameField = fieldMappers.fieldMappers().mapper().indexName();
                }
                return wrapSmartNameQuery(super.getFuzzyQuery(indexedNameField, termStr, minSimilarity), fieldMappers);
            }
        }
        return super.getFuzzyQuery(indexedNameField, termStr, minSimilarity);
    }

    @Override protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    indexedNameField = fieldMappers.fieldMappers().mapper().indexName();
                }
                return wrapSmartNameQuery(super.getWildcardQuery(indexedNameField, termStr), fieldMappers);
            }
        }
        return super.getWildcardQuery(indexedNameField, termStr);
    }

    protected FieldMapper fieldMapper(String smartName) {
        if (mapperService == null) {
            return null;
        }
        FieldMappers fieldMappers = mapperService.smartNameFieldMappers(smartName);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }
}
