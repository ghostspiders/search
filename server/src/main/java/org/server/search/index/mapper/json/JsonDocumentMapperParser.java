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

package org.server.search.index.mapper.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.lucene.document.Field;

import org.apache.lucene.document.FieldType;
import org.server.search.index.analysis.AnalysisService;
import org.server.search.index.mapper.DocumentMapper;
import org.server.search.index.mapper.DocumentMapperParser;
import org.server.search.index.mapper.MapperParsingException;
import org.server.search.util.io.FastStringReader;
import org.server.search.util.io.compression.GZIPCompressor;
import org.server.search.util.io.compression.LzfCompressor;
import org.server.search.util.io.compression.ZipCompressor;
import org.server.search.util.joda.Joda;
import org.server.search.util.json.Jackson;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static org.server.search.index.mapper.json.JsonMapperBuilders.*;

 
public class JsonDocumentMapperParser implements DocumentMapperParser {

    private final ObjectMapper objectMapper = Jackson.newObjectMapper();

    private final AnalysisService analysisService;

    public JsonDocumentMapperParser(AnalysisService analysisService) {
        this.analysisService = analysisService;
    }

    @Override public DocumentMapper parse(String source) throws MapperParsingException {
        return parse(null, source);
    }

    @Override public DocumentMapper parse(String type, String source) throws MapperParsingException {
        JsonNode root;
        try {
            root = objectMapper.readValue(new FastStringReader(source), JsonNode.class);
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse json mapping definition", e);
        }
        String rootName = root.fieldNames().next();
        ObjectNode rootObj;
        if (type == null) {
            // we have no type, we assume the first node is the type
            rootObj = (ObjectNode) root.get(rootName);
            type = rootName;
        } else {
            // we have a type, check if the top level one is the type as well
            // if it is, then the root is that node, if not then the root is the master node
            if (type.equals(rootName)) {
                JsonNode tmpNode = root.get(type);
                if (!tmpNode.isObject()) {
                    throw new MapperParsingException("Expected root node name [" + rootName + "] to be of json object type, but its not");
                }
                rootObj = (ObjectNode) tmpNode;
            } else {
                rootObj = (ObjectNode) root;
            }
        }

        JsonDocumentMapper.Builder docBuilder = JsonMapperBuilders.doc(parseObject(type, rootObj));

        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = rootObj.fields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();

            if ("sourceField".equals(fieldName)) {
                docBuilder.sourceField(parseSourceField((ObjectNode) fieldNode));
            } else if ("idField".equals(fieldName)) {
                docBuilder.idField(parseIdField((ObjectNode) fieldNode));
            } else if ("typeField".equals(fieldName)) {
                docBuilder.typeField(parseTypeField((ObjectNode) fieldNode));
            } else if ("uidField".equals(fieldName)) {
                docBuilder.uidField(parseUidField((ObjectNode) fieldNode));
            } else if ("boostField".equals(fieldName)) {
                docBuilder.boostField(parseBoostField((ObjectNode) fieldNode));
            } else if ("indexAnalyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.textValue()));
            } else if ("searchAnalyzer".equals(fieldName)) {
                docBuilder.searchAnalyzer(analysisService.analyzer(fieldNode.textValue()));
            } else if ("analyzer".equals(fieldName)) {
                docBuilder.indexAnalyzer(analysisService.analyzer(fieldNode.textValue()));
                docBuilder.searchAnalyzer(analysisService.analyzer(fieldNode.textValue()));
            }
        }

        if (!docBuilder.hasIndexAnalyzer()) {
            docBuilder.indexAnalyzer(analysisService.defaultIndexAnalyzer());
        }
        if (!docBuilder.hasSearchAnalyzer()) {
            docBuilder.searchAnalyzer(analysisService.defaultSearchAnalyzer());
        }

        docBuilder.mappingSource(source);

        return docBuilder.build();
    }

    private JsonUidFieldMapper.Builder parseUidField(ObjectNode uidNode) {
        String name = uidNode.get("name") == null ? JsonUidFieldMapper.Defaults.NAME : uidNode.get("name").textValue();
        JsonUidFieldMapper.Builder builder = uid(name);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = uidNode.fields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();

            if ("indexName".equals(fieldName)) {
                builder.indexName(fieldNode.textValue());
            }
        }
        return builder;
    }

    private JsonBoostFieldMapper.Builder parseBoostField(ObjectNode boostNode) {
        String name = boostNode.get("name") == null ? JsonBoostFieldMapper.Defaults.NAME : boostNode.get("name").textValue();
        JsonBoostFieldMapper.Builder builder = boost(name);
        parseNumberField(builder, name, boostNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = boostNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.numberValue().floatValue());
            }
        }
        return builder;
    }

    private JsonTypeFieldMapper.Builder parseTypeField(ObjectNode typeNode) {
        String name = typeNode.get("name") == null ? JsonTypeFieldMapper.Defaults.NAME : typeNode.get("name").textValue();
        JsonTypeFieldMapper.Builder builder = type(name);
        parseJsonField(builder, name, typeNode);
        return builder;
    }


    private JsonIdFieldMapper.Builder parseIdField(ObjectNode idNode) {
        String name = idNode.get("name") == null ? JsonIdFieldMapper.Defaults.NAME : idNode.get("name").textValue();
        JsonIdFieldMapper.Builder builder = id(name);
        parseJsonField(builder, name, idNode);
        return builder;
    }

    private JsonSourceFieldMapper.Builder parseSourceField(ObjectNode sourceNode) {
        String name = sourceNode.get("name") == null ? JsonSourceFieldMapper.Defaults.NAME : sourceNode.get("name").textValue();
        JsonSourceFieldMapper.Builder builder = source(name);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = sourceNode.fields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("compressionThreshold")) {
                builder.compressionThreshold(fieldNode.numberValue().intValue());
            } else if (fieldName.equals("compressionType")) {
                String compressionType = fieldNode.textValue();
                if ("zip".equals(compressionType)) {
                    builder.compressor(new ZipCompressor());
                } else if ("gzip".equals(compressionType)) {
                    builder.compressor(new GZIPCompressor());
                } else if ("lzf".equals(compressionType)) {
                    builder.compressor(new LzfCompressor());
                } else {
                    throw new MapperParsingException("No compressor registed under [" + compressionType + "]");
                }
            }
        }
        return builder;
    }

    private JsonObjectMapper.Builder parseObject(String name, ObjectNode node) {
        JsonObjectMapper.Builder builder = object(name);
        for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = node.fields(); fieldsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = fieldsIt.next();
            String fieldName = entry.getKey();
            JsonNode fieldNode = entry.getValue();
            if (fieldName.equals("dynamic")) {
                builder.dynamic(fieldNode.booleanValue());
            } else if (fieldName.equals("type")) {
                String type = fieldNode.textValue();
                if (!type.equals("object")) {
                    throw new MapperParsingException("Trying to parse an object but has a different type [" + type + "] for [" + name + "]");
                }
            } else if (fieldName.equals("dateFormats")) {
                List<DateTimeFormatter> dateTimeFormatters = newArrayList();
                if (fieldNode.isArray()) {
                    for (JsonNode node1 : (ArrayNode) fieldNode) {
                        dateTimeFormatters.add(parseDateTimeFormatter(fieldName, node1));
                    }
                } else if ("none".equals(fieldNode.textValue())) {
                    dateTimeFormatters = null;
                } else {
                    dateTimeFormatters.add(parseDateTimeFormatter(fieldName, fieldNode));
                }
                if (dateTimeFormatters == null) {
                    builder.noDateTimeFormatter();
                } else {
                    builder.dateTimeFormatter(dateTimeFormatters);
                }
            } else if (fieldName.equals("enabled")) {
                builder.enabled(fieldNode.booleanValue());
            } else if (fieldName.equals("pathType")) {
                builder.pathType(parsePathType(name, fieldNode.textValue()));
            } else if (fieldName.equals("properties")) {
                parseProperties(builder, (ObjectNode) fieldNode);
            }
        }
        return builder;
    }

    private JsonPath.Type parsePathType(String name, String path) throws MapperParsingException {
        if ("justName".equals(path)) {
            return JsonPath.Type.JUST_NAME;
        } else if ("full".equals(path)) {
            return JsonPath.Type.FULL;
        } else {
            throw new MapperParsingException("Wrong value for pathType [" + path + "] for objet [" + name + "]");
        }
    }

    private void parseProperties(JsonObjectMapper.Builder objBuilder, ObjectNode propsNode) {
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = propsNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();

            String type;
            JsonNode typeNode = propNode.get("type");
            if (typeNode != null) {
                type = typeNode.textValue();
            } else {
                // lets see if we can derive this...
                if (propNode.isObject() && propNode.get("properties") != null) {
                    type = "object";
                } else {
                    throw new MapperParsingException("No type specified for property [" + propName + "]");
                }
            }
            if (type.equals("string")) {
                objBuilder.add(parseString(propName, (ObjectNode) propNode));
            } else if (type.equals("date")) {
                objBuilder.add(parseDate(propName, (ObjectNode) propNode));
            } else if (type.equals("integer")) {
                objBuilder.add(parseInteger(propName, (ObjectNode) propNode));
            } else if (type.equals("long")) {
                objBuilder.add(parseLong(propName, (ObjectNode) propNode));
            } else if (type.equals("float")) {
                objBuilder.add(parseFloat(propName, (ObjectNode) propNode));
            } else if (type.equals("double")) {
                objBuilder.add(parseDouble(propName, (ObjectNode) propNode));
            } else if (type.equals("boolean")) {
                objBuilder.add(parseBoolean(propName, (ObjectNode) propNode));
            } else if (type.equals("object")) {
                objBuilder.add(parseObject(propName, (ObjectNode) propNode));
            } else if (type.equals("binary")) {
                objBuilder.add(parseBinary(propName, (ObjectNode) propNode));
            }
        }
    }

    private JsonDateFieldMapper.Builder parseDate(String name, ObjectNode dateNode) {
        JsonDateFieldMapper.Builder builder = dateField(name);
        parseNumberField(builder, name, dateNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = dateNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.textValue());
            } else if (propName.equals("format")) {
                builder.dateTimeFormatter(parseDateTimeFormatter(propName, propNode));
            }
        }
        return builder;
    }


    private JsonIntegerFieldMapper.Builder parseInteger(String name, ObjectNode integerNode) {
        JsonIntegerFieldMapper.Builder builder = integerField(name);
        parseNumberField(builder, name, integerNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = integerNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.numberValue().intValue());
            }
        }
        return builder;
    }

    private JsonLongFieldMapper.Builder parseLong(String name, ObjectNode longNode) {
        JsonLongFieldMapper.Builder builder = longField(name);
        parseNumberField(builder, name, longNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = longNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.numberValue().longValue());
            }
        }
        return builder;
    }

    private JsonFloatFieldMapper.Builder parseFloat(String name, ObjectNode floatNode) {
        JsonFloatFieldMapper.Builder builder = floatField(name);
        parseNumberField(builder, name, floatNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = floatNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.numberValue().floatValue());
            }
        }
        return builder;
    }

    private JsonDoubleFieldMapper.Builder parseDouble(String name, ObjectNode doubleNode) {
        JsonDoubleFieldMapper.Builder builder = doubleField(name);
        parseNumberField(builder, name, doubleNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = doubleNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.numberValue().doubleValue());
            }
        }
        return builder;
    }

    private JsonStringFieldMapper.Builder parseString(String name, ObjectNode stringNode) {
        JsonStringFieldMapper.Builder builder = stringField(name);
        parseJsonField(builder, name, stringNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = stringNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.textValue());
            }
        }
        return builder;
    }

    private JsonBinaryFieldMapper.Builder parseBinary(String name, ObjectNode binaryNode) {
        JsonBinaryFieldMapper.Builder builder = binaryField(name);
        parseJsonField(builder, name, binaryNode);
        return builder;
    }

    private JsonBooleanFieldMapper.Builder parseBoolean(String name, ObjectNode booleanNode) {
        JsonBooleanFieldMapper.Builder builder = booleanField(name);
        parseJsonField(builder, name, booleanNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = booleanNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("nullValue")) {
                builder.nullValue(propNode.booleanValue());
            }
        }
        return builder;
    }

    private void parseNumberField(JsonNumberFieldMapper.Builder builder, String name, ObjectNode numberNode) {
        parseJsonField(builder, name, numberNode);
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = numberNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("precisionStep")) {
                builder.precisionStep(propNode.numberValue().intValue());
            }
        }
    }

    private void parseJsonField(JsonFieldMapper.Builder builder, String name, ObjectNode fieldNode) {
        for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldNode.fields(); propsIt.hasNext();) {
            Map.Entry<String, JsonNode> entry = propsIt.next();
            String propName = entry.getKey();
            JsonNode propNode = entry.getValue();
            if (propName.equals("indexName")) {
                builder.indexName(propNode.textValue());
            } else if (propName.equals("store")) {
                builder.store(parseStore(name, propNode.textValue()));
            } else if (propName.equals("index")) {
                builder.index(parseIndex(name, propNode.textValue()));
            } else if (propName.equals("termVector")) {
                builder.termVector(parseTermVector(name, propNode.textValue()));
            } else if (propName.equals("boost")) {
                builder.boost(propNode.numberValue().floatValue());
            } else if (propName.equals("omitNorms")) {
                builder.omitNorms(propNode.booleanValue());
            } else if (propName.equals("omitTermFreqAndPositions")) {
                builder.omitTermFreqAndPositions(propNode.booleanValue());
            } else if (propName.equals("indexAnalyzer")) {
                builder.indexAnalyzer(analysisService.analyzer(propNode.textValue()));
            } else if (propName.equals("searchAnalyzer")) {
                builder.searchAnalyzer(analysisService.analyzer(propNode.textValue()));
            } else if (propName.equals("analyzer")) {
                builder.indexAnalyzer(analysisService.analyzer(propNode.textValue()));
                builder.searchAnalyzer(analysisService.analyzer(propNode.textValue()));
            }
        }
    }

    private DateTimeFormatter parseDateTimeFormatter(String fieldName, JsonNode node) {
        if (node.isTextual()) {
            return Joda.forPattern(node.textValue()).withZone(DateTimeZone.UTC);
        } else {
            // TODO support more complex configuration...
            throw new MapperParsingException("Wrong node to use to parse date formatters [" + fieldName + "]");
        }
    }

    private FieldType parseTermVector(String fieldName, String termVector) throws MapperParsingException {
        if ("no".equals(termVector)) {
            return new FieldType();
        } else if ("yes".equals(termVector)) {
            return new FieldType();
        } else if ("with_offsets".equals(termVector)) {
            return new FieldType();
        } else if ("with_positions".equals(termVector)) {
            return new FieldType();
        } else if ("with_positions_offsets".equals(termVector)) {
            return new FieldType();
        } else {
            throw new MapperParsingException("Wrong value for termVector [" + termVector + "] for field [" + fieldName + "]");
        }
    }

    private FieldType parseIndex(String fieldName, String index) throws MapperParsingException {
        if ("no".equals(index)) {
            return new FieldType();
        } else if ("not_analyzed".equals(index)) {
            return new FieldType();
        } else if ("analyzed".equals(index)) {
            return new FieldType();
        } else {
            throw new MapperParsingException("Wrong value for index [" + index + "] for field [" + fieldName + "]");
        }
    }

    private Field.Store parseStore(String fieldName, String store) throws MapperParsingException {
        if ("no".equals(store)) {
            return Field.Store.NO;
        } else if ("yes".equals(store)) {
            return Field.Store.YES;
        } else {
            throw new MapperParsingException("Wrong value for store [" + store + "] for field [" + fieldName + "]");
        }
    }
}
