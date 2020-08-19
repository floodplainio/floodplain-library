/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.immutable.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;
import io.floodplain.immutable.api.ImmutableTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

public class ImmutableJSON {

    private static final Logger logger = LoggerFactory.getLogger(ImmutableJSON.class);
    public static final ObjectMapper objectMapper = new ObjectMapper();

    private ImmutableJSON() {
        // -- no instances
    }

    public static ObjectNode json(ImmutableMessage msg) throws IOException {
        ObjectNode node = objectMapper.createObjectNode();
        msg.toTypedDataMap().entrySet().forEach(e -> resolveValue(objectMapper, node, e.getKey(), e.getValue().type, Optional.ofNullable(e.getValue().value), false));
        for (Entry<String, List<ImmutableMessage>> e : msg.subMessageListMap().entrySet()) {

            String key = e.getKey();
            ArrayNode nd = objectMapper.createArrayNode();
            for (ImmutableMessage submsg : e.getValue()) {
                nd.add(json(submsg));
            }
            node.set(key, nd);
        }
        for (Entry<String, ImmutableMessage> e : msg.subMessageMap().entrySet()) {
            node.set(e.getKey(), json(e.getValue()));
        }
        return node;
    }

    public static String ndJson(ImmutableMessage msg) throws IOException {
        return objectMapper.writeValueAsString(json(msg));
    }


    public static byte[] jsonSerializer(ImmutableMessage msg, boolean includeNullValues, boolean includeTypes) {
        try {
            ObjectWriter w = objectMapper.writerWithDefaultPrettyPrinter();
            return w.writeValueAsBytes(ImmutableJSON.toJSON(msg, includeNullValues, includeTypes));
        } catch (JsonProcessingException e) {
            logger.error("JSON parsing failing with value: {} types: {} submessagenames: {}", msg.values(), msg.types(), msg.subMessageMap().keySet());
            logger.error("Error serializing, got json error:", e);
            if (e.getCause() != null) {
                logger.error("Error serializing:", e);
            }
            try {
                logger.error("JSON failed to write: {}", ImmutableJSON.toJSON(msg, includeNullValues, includeTypes));
            } catch (Throwable t) {
                //
            }

            return ("{}").getBytes(StandardCharsets.UTF_8);
        }
    }

    public static ObjectNode toJSON(ImmutableMessage msg, boolean includeNullValues, boolean includeTypes) {
        ObjectNode result = objectMapper.createObjectNode();
        msg.columnNames().forEach(column -> {
            ValueType type = msg.columnType(column);
            result.put(column + ".type", ImmutableTypeParser.typeName(type));
            resolveValue(objectMapper, result, column, type, msg.value(column), includeNullValues);
        });
        msg.subMessageListMap().entrySet().forEach(e -> {
            String key = e.getKey();
            ArrayNode nd = objectMapper.createArrayNode();
            e.getValue().stream().map(x -> toJSON(x, includeNullValues, includeTypes)).forEach(nd::add);
            result.set(key, nd);
        });
        msg.subMessageMap().entrySet().forEach(e -> {
            String key = e.getKey();
            result.set(key, toJSON(e.getValue(), includeNullValues, includeTypes));
        });
        return result;
    }


    private static void resolveValue(ObjectMapper objectMapper, ObjectNode m, String key, ValueType type, Optional<Object> maybeValue, boolean includeNullValues) {
        if (!maybeValue.isPresent()) {
            if (includeNullValues) {
                m.putNull(key);
            }
            return;
        }
        if (type == null) {
            logger.error("Null type for key: {}", key);
            return;
        }
        Object value = maybeValue.get();
        switch (type) {
            case STRING:
            case BINARY_DIGEST:
                m.put(key, (String) value);
                return;
            case INTEGER:
                m.put(key, (Integer) value);
                return;
            case LONG:
                m.put(key, (Long) value);
                return;
            case DOUBLE:
                m.put(key, (Double) value);
                return;
            case FLOAT:
                m.put(key, (Float) value);
                return;
            case BOOLEAN:
                m.put(key, (Boolean) value);
                return;
            case DATE:
                if (value instanceof String) {
                    m.put(key, (String) value);
                } else {
                    String t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS").format((Date) value);
                    m.put(key, t);
                }
                return;
            case CLOCKTIME:
                if (value instanceof String) {
                    m.put(key, (String) value);
                } else {
                    String c = new SimpleDateFormat("HH:mm:ss").format((Date) value);
                    m.put(key, c);
                }

                return;
            case STRINGLIST:
                if (value instanceof String[]) {
                    String[] values = (String[]) value;
                    ArrayNode arrayNode = m.putArray(key);
                    ArrayNode valueToTree = objectMapper.valueToTree(values);
                    arrayNode.addAll(valueToTree);
                }
                return;
            default:
                break;
        }

    }


  }


