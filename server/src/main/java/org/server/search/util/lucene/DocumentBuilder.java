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

package org.server.search.util.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;


public class DocumentBuilder {
    public static DocumentBuilder doc() {
        return new DocumentBuilder();
    }

    public static FieldBuilder field(String name, String value) {
        FieldType ft = new FieldType(TextField.TYPE_STORED);
        return field(name, value, ft);
    }

    public static FieldBuilder field(String name, String value, FieldType type) {
        return new FieldBuilder(name, value, type);
    }



    public static FieldBuilder field(String name, byte[] value, FieldType type) {
        return new FieldBuilder(name, value, type);
    }

    public static FieldBuilder field(String name, byte[] value, int offset, int length, FieldType type) {
        return new FieldBuilder(name, value, offset, length, type);
    }

    private final Document document;

    private DocumentBuilder() {
        this.document = new Document();
    }

    public DocumentBuilder boost(float boost) {
        return this;
    }

    public DocumentBuilder add(Field field) {
        document.add(field);
        return this;
    }

    public DocumentBuilder add(FieldBuilder fieldBuilder) {
        document.add(fieldBuilder.build());
        return this;
    }

    public Document build() {
        return document;
    }
}
