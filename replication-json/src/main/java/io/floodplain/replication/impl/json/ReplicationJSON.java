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
package io.floodplain.replication.impl.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.TypedData;
import io.floodplain.immutable.api.ImmutableTypeParser;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.DateSerializer;
import io.floodplain.replication.factory.ReplicationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.Map.Entry;

import static io.floodplain.immutable.api.ImmutableMessage.ValueType;

public class ReplicationJSON {

    private final static Logger logger = LoggerFactory.getLogger(ReplicationJSON.class);
    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static final DateTimeFormatter clocktimeFormatter =  DateTimeFormatter.ofPattern("HH:mm:ss");

    public static ReplicationMessage parseReplicationMessage(byte[] data, Optional<String> source) throws IOException {
        return ReplicationJSON.parseJSON(source, (ObjectNode) parseJSON(data));
    }

    public static byte[] jsonSerializer(ReplicationMessage msg, boolean includeNullValues) {
        return jsonSerializer(msg, includeNullValues, false);
    }

    public static byte[] jsonSerializer(ReplicationMessage msg, boolean includeNullValues, boolean dumpKey) {
        try {
            ObjectWriter w = ReplicationMessage.usePrettyPrint() ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            if (dumpKey) {
                baos.write(msg.combinedKey().getBytes(StandardCharsets.UTF_8));
                baos.write('\t');
            }
            w.writeValue(baos, ReplicationJSON.toJSON(msg, includeNullValues));
            baos.write('\n');
            return baos.toByteArray();
        } catch (JsonProcessingException e) {
            logger.error("JSON parsing failing with key: {} value: {} submessagenames: {}", msg.queueKey(), msg.values(), msg.message().subMessageMap().keySet());
            logger.error("Error serializing, got json error:", e);
            if (e.getCause() != null) {
                logger.error("Error serializing cause:", e.getCause());
                e.printStackTrace();
            }
            try {
                logger.error("JSON failed to write: {}", ReplicationJSON.toJSON(msg, includeNullValues).toString());
            } catch (Throwable t) {
                logger.error("Logging issue: ",t);
            }

            return ("{}").getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Weird json problem", e);
        }
    }

    public static String jsonDescriber(ReplicationMessage msg) {
        return msg.flatValueMap(true, Collections.emptySet(), "").toString();
    }

    private static JsonNode parseJSON(byte[] data) throws IOException {
        try {
            return objectMapper.readTree(data);
        } catch (Throwable t) {
            logger.error("Exception on parsing json! {}", new String(data, StandardCharsets.UTF_8), t);
            throw t;
        }

    }

    static void resolveValue(ObjectNode m, String key, ValueType type, Object value, boolean includeNullValues) {
        if (value == null) {
            if (includeNullValues) {
                m.putNull(key);
            }
            return;
        }
        // coming across optionals. Shouldn't be TODO investigate
        // Treat empty optionals as null, barf on filled optionals
        if (value instanceof Optional) {
            Optional<?> o = (Optional<?>) value;
            // empty optional, treat as null
            if (o.isEmpty()) {
                if (includeNullValues) {
                    m.putNull(key);
                }
                return;
            }
            throw new RuntimeException("Unexpected optional for " + key + " type: " + type);
        }
        if (type == null) {
            throw new IllegalArgumentException("Null type for key: " + key);
        }
        switch (type) {
            case STRING:
            case BINARY_DIGEST:
                m.put("Value", (String) value);
                return;
            case DECIMAL:
                m.put("Value",((BigDecimal)value).toPlainString());
                return;
            case INTEGER:
                m.put("Value", (Integer) value);
                return;
            case LONG:
                m.put("Value", (Long) value);
                return;
            case DOUBLE:
                m.put("Value", (Double) value);
                return;
            case FLOAT:
                if (value instanceof Float) {
                    m.put("Value", (Float) value);
                } else {
                    m.put("Value", (Double) value);
                }
                return;
            case BOOLEAN:
                m.put("Value", (Boolean) value);
                return;
            case DATE:
            case TIMESTAMP:
            case CLOCKTIME:
                if (value instanceof String) {
                    m.put("Value", (String) value);
                } else if (value instanceof Temporal) {
                    m.put("Value", DateSerializer.serializeTimeObject((Temporal) value));
                }
                return;
            case STRINGLIST:
                ArrayNode stringArrayNode = m.putArray("Value");
                if (value instanceof String[]) {
                    ArrayNode conf = objectMapper.valueToTree(value);
                    stringArrayNode.addAll(conf);
                } else if (value instanceof List) {
                    ArrayNode conf = objectMapper.valueToTree(value);
                    stringArrayNode.addAll(conf);
                }
                break;
            case BINARY:
                m.put("Value", Base64.getEncoder().encodeToString((byte[]) value));
                break;
            case ENUM:
                m.put("Value", (String) value);
                break;
            default:
                logger.warn("Unknown type: {} while serializing replication message to JSON. ", type);
                break;
        }
    }


    public static Object resolveValue(ValueType type, JsonNode jsonNode) {
        if (jsonNode == null) {
            return null;
        }
        if (jsonNode instanceof NullNode) {
            return null;
        }

        switch (type) {
            case STRING:
            case BINARY_DIGEST:
            case ENUM:
                return jsonNode.asText();
            case DECIMAL:
                // No idea if this is correct
                return new BigDecimal(jsonNode.asText());
            case INTEGER:
                return jsonNode.asInt();
            case LONG:
                return jsonNode.asLong();
            case DOUBLE:
            case FLOAT:
                return jsonNode.asDouble();
            case BOOLEAN:
                return jsonNode.asBoolean();
            case BINARY:
                return jsonNode.isNull() ? new byte[]{} : Base64.getDecoder().decode(jsonNode.asText());
            case STRINGLIST:
                ArrayNode stringNode = ((ArrayNode) jsonNode);
                List<String> stringResult = new ArrayList<>();
                for (final JsonNode objNode : stringNode) {
                    if (objNode.isTextual()) {
                        stringResult.add(objNode.asText());
                    } else {
                        logger.warn("Unsupported array element type: {} in {}. Ignoring!", objNode, jsonNode);
                    }
                }
                return stringResult;
            case DATE:
            case TIMESTAMP:
            case CLOCKTIME:
                return DateSerializer.parseTimeObject(jsonNode.asText());

            default:
                logger.warn("Unsupported type {}", type);
                break;
        }
        return null;
    }

    public static ObjectNode toJSON(ReplicationMessage instance, boolean includeNullValues) {
        ObjectNode node = objectMapper.createObjectNode();
        try {
            if (instance.transactionId() != null) {
                node.put("TransactionId", instance.transactionId());
            }
            node.put("Timestamp", instance.timestamp());
            node.put("Operation", instance.operation().toString());
            ArrayNode keys = objectMapper.createArrayNode();
            for (String key : instance.primaryKeys()) {
                keys.add(key);
            }
            node.set("PrimaryKeys", keys);
            ImmutableMessage immutable = instance.message();

            appendImmutable(node, immutable, includeNullValues);
            Optional<ImmutableMessage> paramMessage = instance.paramMessage();
            if (paramMessage.isPresent()) {
                ObjectNode paramNode = objectMapper.createObjectNode();
                node.set("ParamMessage", paramNode);
                appendImmutable(paramNode, paramMessage.get(), includeNullValues);
            }
        } catch (Throwable e) {
            logger.error("Error serializing replicationmessage " + instance.queueKey() + " columns: " + instance.values(), e);
        }
        return node;

    }

    public static ObjectNode replicationToJSON(ReplicationMessage msg) {
        ObjectNode node = objectMapper.createObjectNode();
        node.set("payload", immutableToJSON(msg.message()));
        ObjectNode schema = objectMapper.createObjectNode();
        node.set("schema", schema);
        return node;
    }

    public static byte[] replicationToConnectJSON(ReplicationMessage msg) {
        ObjectNode node = replicationToJSON(msg);
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error writing json", e);
        }
    }

    public static byte[] immutableTotalToJSON(ImmutableMessage msg) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            appendImmutable(node, msg, false);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error writing json", e);
        }
    }

    private static ObjectNode immutableToJSON(ImmutableMessage msg) {
        ObjectNode node = objectMapper.createObjectNode();
        Map<String, TypedData> values = msg.toTypedDataMap();
        for (Entry<String, TypedData> e : values.entrySet()) {
            Object o = e.getValue().value;
            String key = e.getKey();
            if (o == null) {
                continue;
            }
            switch (e.getValue().type) {

                case BOOLEAN:
                    node.put(key, (Boolean) o);
                    break;
                case DATE:
                case TIMESTAMP:
                case CLOCKTIME:
                    node.put(key, DateSerializer.serializeTimeObject((Temporal) o));
                    break;
                case DECIMAL:
                    node.put(key,((BigDecimal)o).toPlainString());
                    break;
                case DOUBLE:
                case FLOAT:
                    node.put(key, (Double) o);
                    break;
                case INTEGER:
                    node.put(key, (Integer) o);
                    break;
                case STRINGLIST:
                    String[] rs = (String[]) o;
                    ArrayNode alist = objectMapper.createArrayNode();
                    for (String element : rs) {
                        alist.add(element);
                    }
                    node.set(key, alist);
                    break;
                case LONG:
                    node.put(key, (Long) o);
                    break;
                case STRING:
                    node.put(key, (String) o);
                    break;
                case ENUM:
                    node.put(key, o.toString());
                    break;
                case IMMUTABLE:
                    break;
                case BINARY:
                    // ignore binaries for now
                    break;
                case BINARY_DIGEST:
                case STOPWATCHTIME:
                    throw new RuntimeException("Whoops, illegal type: " + e.getValue().type);
                    // ignore this one:
                case UNKNOWN:
                    break;
                default:
                    break;
            }

        }
        msg.subMessageMap().forEach((key, value) -> node.set(key, immutableToJSON(value)));
        msg.subMessageListMap().forEach((key, value) -> {
            ArrayNode al = objectMapper.createArrayNode();
            value.stream().map(ReplicationJSON::immutableToJSON).forEach(al::add);
            node.set(key, al);
        });
        return node;
    }

    private static void appendImmutable(ObjectNode node, ImmutableMessage immutable, boolean includeNullValues) throws JsonProcessingException {
        ObjectNode columns = objectMapper.createObjectNode();
        node.set("Columns", columns);
        final Map<String, ValueType> types = immutable.types();
        for (Entry<String, Object> e : immutable.values().entrySet()) {
            final String key = e.getKey();
            final ValueType type = types.get(key);
            Object value = e.getValue();
            if (type == ValueType.UNKNOWN && value != null) {
                String msg = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
                logger.error("Unknown type detected while appending key: {} and node: {}", key, msg);
            }
            // Specifically skip untyped nulls. Typed nulls are ok, untyped non-nulls will throw an exception from typeName
            if (value != null || type != ValueType.UNKNOWN) {
                ObjectNode m = objectMapper.createObjectNode();
                m.put("Type", ImmutableTypeParser.typeName(type));
                ReplicationJSON.resolveValue(m, key, type, value, includeNullValues);
                columns.set(e.getKey(), m);
            }
        }
        ObjectNode subMessage = null;
        if (immutable.subMessageMap() != null || immutable.subMessageListMap() != null) {
            subMessage = objectMapper.createObjectNode();
            node.set("SubMessage", subMessage);
        }
        if (subMessage != null) {
            if (immutable.subMessageMap() != null) {
                for (Entry<String, ImmutableMessage> entry : immutable.subMessageMap().entrySet()) {
                    subMessage.set(entry.getKey(), toJSONImmutable(entry.getValue(), includeNullValues));
                }
            }
            if (immutable.subMessageListMap() != null) {
                for (Entry<String, List<ImmutableMessage>> entry : immutable.subMessageListMap().entrySet()) {
                    ArrayNode arraynode = objectMapper.createArrayNode();
                    subMessage.set(entry.getKey(), arraynode);
                    List<ImmutableMessage> list = entry.getValue();
                    for (ImmutableMessage msg : list) {
                        arraynode.add(toJSONImmutable(msg, includeNullValues));
                    }
                }
            }
        }
    }

    private static ObjectNode toJSONImmutable(ImmutableMessage immutable, boolean includeNullValues) {
        ObjectNode node = objectMapper.createObjectNode();
        try {
            node.put("TransactionId", "");
            node.put("Timestamp", -1L);
            node.put("Operation", ReplicationMessage.Operation.NONE.name());
            ArrayNode keys = objectMapper.createArrayNode();
            node.set("PrimaryKeys", keys);
            appendImmutable(node, immutable, includeNullValues);
        } catch (Throwable e) {
            logger.error("Error serializing replicationmessage.", e);
            e.printStackTrace();
        }
        return node;
    }

    public static ReplicationMessage parseJSON(Optional<String> source, ObjectNode node) {

        final String transactionId = node.get("TransactionId") != null ? node.get("TransactionId").asText() : null;
        final long timestamp = node.get("Timestamp") != null ? node.get("Timestamp").asLong() : -1;
        final ReplicationMessage.Operation operation = node.get("Operation") != null ? ReplicationMessage.Operation.valueOf(node.get("Operation").asText().toUpperCase()) : ReplicationMessage.Operation.NONE;
        ArrayNode keys = (ArrayNode) node.get("PrimaryKeys");
        List<String> l;
        if (keys != null) {
            l = new ArrayList<>();
            for (JsonNode key : keys) {
                l.add(key.asText());
            }
        } else {
            l = Collections.emptyList();
        }
        final List<String> primaryKeys = Collections.unmodifiableList(l);


        ImmutableMessage flatMessage = parseImmutable(node);
        ObjectNode paramMsg = (ObjectNode) node.get("ParamMessage");
        return ReplicationFactory.createReplicationMessage(source, Optional.empty(), Optional.empty(), transactionId, timestamp, operation, primaryKeys, flatMessage, Optional.empty(), Optional.ofNullable(paramMsg).map(ReplicationJSON::parseImmutable));

    }

    public static ImmutableMessage parseImmutable(byte[] data) throws IOException {
        return parseImmutable((ObjectNode) parseJSON(data));

    }


    private static ImmutableMessage parseImmutable(ObjectNode node) {
        ObjectNode val = (ObjectNode) node.get("Columns");
        Map<String, Object> localvalues = new HashMap<>();
        Map<String, ValueType> localtypes = new HashMap<>();
        if (val != null) {
            Iterator<Entry<String, JsonNode>> it = val.fields();
            while (it.hasNext()) {
                Entry<String, JsonNode> e = it.next();
                String name = e.getKey();
                ObjectNode column = (ObjectNode) e.getValue();
                JsonNode typeObject = column.get("Type");
                if (typeObject == null) {
                    try {
                        logger.error("Null type found in replication message: " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node));
                    } catch (JsonProcessingException e1) {
                        logger.error("Error: ", e1);
                    }
                    throw new IllegalArgumentException("Missing type in json!");
                }
                ValueType type = ImmutableTypeParser.parseType(typeObject.asText());
                if (type == ValueType.UNKNOWN) {
                    try {
                        String serialized = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
                        logger.error("Unknown type detected while parsing JSON: {}", serialized);
                    } catch (JsonProcessingException ex) {
                        ex.printStackTrace();
                    }
                }
                Object value = ReplicationJSON.resolveValue(type, column.get("Value"));
                localvalues.put(name, value);
                localtypes.put(name, type);
            }
        }
        Map<String, ValueType> types = Collections.unmodifiableMap(localtypes);
        Map<String, Object> values = Collections.unmodifiableMap(localvalues);
        ObjectNode subMessage = (ObjectNode) node.get("SubMessage");

        Map<String, ImmutableMessage> subMessageMap;
        Map<String, List<ImmutableMessage>> subMessageListMap;

        if (subMessage != null) {
            Map<String, ImmutableMessage> inProgressSubMessageMap = null;
            Map<String, List<ImmutableMessage>> inProgressSubMessageListMap = null;
            Iterator<Entry<String, JsonNode>> it = subMessage.fields();
            while (it.hasNext()) {
                Entry<String, JsonNode> e = it.next();
                String name = e.getKey();
                JsonNode json = e.getValue();
                if (json instanceof ObjectNode) {
                    ObjectNode sub = (ObjectNode) json;
                    ImmutableMessage subMessageImpl = parseImmutable(sub);
                    if (inProgressSubMessageMap == null) {
                        inProgressSubMessageMap = new HashMap<>();
                    }
                    inProgressSubMessageMap.put(name, subMessageImpl);
                } else if (json instanceof ArrayNode) {
                    ArrayNode sub = (ArrayNode) json;
                    List<ImmutableMessage> msgs = new LinkedList<>();
                    for (JsonNode element : sub) {
                        ImmutableMessage subMessageImpl = parseImmutable((ObjectNode) element);
                        msgs.add(subMessageImpl);
                    }
                    if (inProgressSubMessageListMap == null) {
                        inProgressSubMessageListMap = new HashMap<>();
                    }
                    inProgressSubMessageListMap.put(name, Collections.unmodifiableList(msgs));
                }

            }
            subMessageMap = inProgressSubMessageMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(inProgressSubMessageMap);
            subMessageListMap = inProgressSubMessageListMap == null ? Collections.emptyMap() : Collections.unmodifiableMap(inProgressSubMessageListMap);
        } else {
            subMessageMap = Collections.emptyMap();
            subMessageListMap = Collections.emptyMap();
        }
        return ImmutableFactory.create(values, types, subMessageMap, subMessageListMap);
    }


//	public ReplicationMessageImpl(ReplicationMessageImpl parent, Operation operation, long timestamp, Map<String,Object> values, Map<String,String> types,Map<String,ReplicationMessage> submessage, Map<String,List<ReplicationMessage>> submessages, Optional<List<String>> primaryKeys) {

}


