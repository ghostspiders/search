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

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.server.search.search.SearchParseElement;
import org.server.search.search.internal.SearchContext;

import java.util.ArrayList;

/**
 * @author kimchy (Shay Banon)
 */
public class FieldsParseElement implements SearchParseElement {

    @Override public void parse(JsonParser jp, SearchContext context) throws Exception {
        JsonToken token = jp.getCurrentToken();
        if (token == JsonToken.START_ARRAY) {
            jp.nextToken();
            ArrayList<String> fieldNames = new ArrayList<String>();
            do {
                fieldNames.add(jp.getText());
            } while ((token = jp.nextToken()) != JsonToken.END_ARRAY);
            context.fieldNames(fieldNames.toArray(new String[fieldNames.size()]));
        } else if (token == JsonToken.VALUE_STRING) {
            context.fieldNames(new String[]{jp.getText()});
        }
    }
}
