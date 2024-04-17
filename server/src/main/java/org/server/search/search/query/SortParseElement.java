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

package org.server.search.search.query;

import com.google.common.collect.Lists;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.server.search.index.mapper.FieldMappers;
import org.server.search.search.SearchParseElement;
import org.server.search.search.SearchParseException;
import org.server.search.search.internal.SearchContext;
import org.server.search.util.gnu.trove.TObjectIntHashMap;
import org.server.search.util.trove.ExtTObjectIntHasMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class SortParseElement implements SearchParseElement {

    private final Map<String,SortField.Type> sortFieldTypesMapper = new HashMap<>();

    private static final SortField SORT_SCORE = new SortField(null, SortField.Type.SCORE);
    private static final SortField SORT_SCORE_REVERSE = new SortField(null, SortField.Type.SCORE, true);
    private static final SortField SORT_DOC = new SortField(null, SortField.Type.DOC);
    private static final SortField SORT_DOC_REVERSE = new SortField(null, SortField.Type.DOC, true);

    public SortParseElement() {
        sortFieldTypesMapper.put("string", SortField.Type.STRING);
        sortFieldTypesMapper.put("int", SortField.Type.INT);
        sortFieldTypesMapper.put("float", SortField.Type.FLOAT);
        sortFieldTypesMapper.put("long", SortField.Type.LONG);
        sortFieldTypesMapper.put("double", SortField.Type.DOUBLE);
        sortFieldTypesMapper.put("short", SortField.Type.INT);
        sortFieldTypesMapper.put("byte", SortField.Type.INT);
        sortFieldTypesMapper.put("string_val", SortField.Type.STRING_VAL);
    }

    @Override public void parse(JsonParser jp, SearchContext context) throws Exception {
        JsonToken token;
        List<SortField> sortFields = Lists.newArrayListWithCapacity(2);
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                String fieldName = jp.getCurrentName();
                boolean reverse = false;
                String innerJsonName = null;
                SortField.Type type = null;
                while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                    if (token == JsonToken.FIELD_NAME) {
                        innerJsonName = jp.getCurrentName();
                    } else if (token == JsonToken.VALUE_TRUE) {
                        if ("reverse".equals(innerJsonName)) {
                            reverse = true;
                        }
                    } else {
                        if ("type".equals(innerJsonName)) {
                            type = sortFieldTypesMapper.get(jp.getText());
                            if (type == null) {
                                throw new SearchParseException("No sort type for [" + jp.getText() + "] with field [" + fieldName + "]");
                            }
                        }
                    }
                }
                if ("score".equals(fieldName)) {
                    if (reverse) {
                        sortFields.add(SORT_SCORE_REVERSE);
                    } else {
                        sortFields.add(SORT_SCORE);
                    }
                } else if ("doc".equals(fieldName)) {
                    if (reverse) {
                        sortFields.add(SORT_DOC_REVERSE);
                    } else {
                        sortFields.add(SORT_DOC);
                    }
                } else {
                    FieldMappers fieldMappers = context.mapperService().smartNameFieldMappers(fieldName);
                    if (fieldMappers == null || fieldMappers.mappers().isEmpty()) {
                        if (type == null) {
                            throw new SearchParseException("No built in mapping found for [" + fieldName + "], and no explicit type defined");
                        }
                    } else {
                        fieldName = fieldMappers.mappers().get(0).indexName();
                        if (type == null) {
                            type = fieldMappers.mappers().get(0).sortType();
                        }
                    }
                    sortFields.add(new SortField(fieldName, type, reverse));
                }
            }
        }
        if (!sortFields.isEmpty()) {
            context.sort(new Sort(sortFields.toArray(new SortField[sortFields.size()])));
        }
    }
}
