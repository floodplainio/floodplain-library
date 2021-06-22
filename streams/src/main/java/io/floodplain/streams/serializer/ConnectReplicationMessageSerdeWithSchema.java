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
package io.floodplain.streams.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.debezium.DebeziumParseException;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import io.floodplain.streams.debezium.TableIdentifier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

public class ConnectReplicationMessageSerdeWithSchema implements Serde<ReplicationMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectReplicationMessageSerdeWithSchema.class);
    private static final ConnectKeySerde keySerde = new ConnectKeySerde();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }


    private static String parseConnectKey(byte[] input) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(input);
        ObjectNode node = (ObjectNode) jsonNode;
        TableIdentifier id = JSONToReplicationMessage.processDebeziumKey(node);
        return id.combinedKey;
    }

    public static Serializer<String> keySerialize() {
        return keySerde.serializer();
    }

    public static Deserializer<String> keyDeserialize() {
        return new Deserializer<>() {

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> config, boolean isKey) {
                logger.info("Configuring key deserializer: {}", config);

            }

            @Override
            public String deserialize(String topic, byte[] data) {
                try {
                    return parseConnectKey(data);
                } catch (IOException e) {
                    String raw = new String(data, StandardCharsets.UTF_8);
                    throw new RuntimeException("Error deserializing key: " + raw, e);
                }
            }
        };
    }

    @Override
    public Deserializer<ReplicationMessage> deserializer() {
        return new Deserializer<>() {

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> config, boolean isKey) {
                logger.info("Configuring deserializer: {}", config);

            }

            @Override
            public ReplicationMessage deserialize(String topic, byte[] data) {
                try {

//                    JSONToReplicationMessage.convertToReplication(false,objectMapper.readTree(data))
                    return JSONToReplicationMessage.processDebeziumBody(data, Optional.of(topic));
                } catch (DebeziumParseException e) {
                    throw new RuntimeException("Error parsing replmessage", e);
                }
//                return parser.parseBytes(Optional.of(topic), data);
            }
        };
    }

    private Object processValue(String name, ImmutableMessage.ValueType type, Object value) {
        switch (type) {
            case DATE:
            case CLOCKTIME:
            case STOPWATCHTIME:
            case TIMESTAMP:
                return processDateType(type,value);
        }
        return value;
    }
    private Map<String,Object> describeType(String name, ImmutableMessage.ValueType type, Object value) {
        Map<String,Object> result = new HashMap<>();
        result.put("optional",true);
        result.put("field",name);
        switch (type) {
            case STRING:
            case BINARY_DIGEST:
            case ENUM:
            case STRINGLIST:
                result.put("type","string");
                break;
            case INTEGER:
                result.put("type","int32");
                break;
            case LONG:
                result.put("type","int64");
                break;
            case DOUBLE:
                result.put("type","float64");
                break;
            case DECIMAL:
                result.put("type","bytes");
                result.put("name", "org.apache.kafka.connect.data.Decimal");
                result.put("version",1);
                BigDecimal bigDec = (BigDecimal) value;
                result.put("parameters", Map.of("scale",bigDec.scale(),"connect.decimal.precision",bigDec.precision()));
                break;
            case FLOAT:
                result.put("type","float32");
                break;
            case BOOLEAN:
                result.put("type","boolean");
                break;
            case DATE:
            case CLOCKTIME:
            case STOPWATCHTIME:
            case TIMESTAMP:
                processDateType(type,value,result);
                break;
            case BINARY:
                result.put("type","bytes");
                break;
            case IMMUTABLE:
            case UNKNOWN:
            case IMMUTABLELIST:
                throw new IllegalArgumentException("Can not serialize type: "+type);
        }
        return result;
    }

    private void processDateType(ImmutableMessage.ValueType type, Object value, Map<String, Object> result) {
        // not sure if I'm gonna need the type field
        if(value==null) {
            logger.warn("Null value for procesDateType. Ignoring");
            return;
        }
        if(value instanceof LocalDateTime) {
            result.put("type","int64");
            result.put("version",1);
            result.put("name", "io.debezium.time.MicroTimestamp");
            return;
        }
        if(value instanceof LocalDate) {
            result.put("type","int32");
            result.put("version",1);
            result.put("name", "io.debezium.time.Date");
            return;
        }
        if(value instanceof LocalTime) {
            result.put("type","int32");
            result.put("version",1);
            result.put("name", "io.debezium.time.Time");
            return;
        }
        throw new IllegalArgumentException("Unsupported type: "+value.getClass().getName()+" for processDateType");
    }

    private Object processDateType(ImmutableMessage.ValueType type, Object value) {
        // not sure if I'm gonna need the type field
        if(value==null) {
            logger.warn("Null value for procesDateType. Ignoring");
            return null;
        }
        if(value instanceof LocalDateTime) {
            LocalDateTime l = ((LocalDateTime)value);
            return l.toEpochSecond(ZoneOffset.UTC);
        }
        if(value instanceof LocalDate) {
            // Is it ok to return an int64 here?
            return ((LocalDate)value).toEpochDay();
        }
        if(value instanceof LocalTime) {
            LocalTime l = ((LocalTime)value);
            return l.toSecondOfDay();
        }
        throw new IllegalArgumentException("Unsupported type: "+value.getClass().getName()+" for processDateType");
    }

    private Map<String,?> buildSchema(String name, ImmutableMessage message) {
        List<?> types = message.types()
                .entrySet()
                .stream()
                .map(e->describeType(e.getKey(),e.getValue(),message.value(e.getKey()).orElse(null)))
                .collect(Collectors.toList());

//        List<?> submessage = message.subMessageMap()
//                .entrySet()
//                .stream()
//                .map(e->buildSchema(e.getKey(),e.getValue()))
//                .collect(Collectors.toList());
        return Map.of("type","struct","optional",true,"fields",types);
//        Map<String,?> subm = Map.of("optional",true,)
    }

    @Override
    public Serializer<ReplicationMessage> serializer() {

        final boolean debug = false;

        return new Serializer<>() {
            boolean schemaEnable = true;

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                Boolean setSchemaEnable = (Boolean) configs.get("schemaEnable");
                if(setSchemaEnable!=null) {
                    schemaEnable = setSchemaEnable;
                }
                logger.info("Configuring: {}", configs);
            }

            @Override
            public byte[] serialize(String topic, ReplicationMessage replMessage) {
                if (replMessage == null || replMessage.operation() == ReplicationMessage.Operation.DELETE) {
                    return null;
                }
                ImmutableMessage message = replMessage.message();
                Map<String, ?> valueMap = message.valueMap(true, Collections.emptySet(),Collections.emptyList());
                if (schemaEnable) {
                    Map<Object,Object> processed = valueMap.entrySet().stream()
                            .filter(e->e.getKey()!=null && e.getValue()!=null)
                            .filter(e->message.columnType(e.getKey())!=null)
                            .map(e->{
                                Object processedValue = processValue(e.getKey(),message.columnType(e.getKey()),e.getValue());
                                logger.info("KEYY: "+e.getKey()+" VALLUE: "+e.getValue()+"PROCESSED: "+processedValue+" TYPE: "+message.columnType(e.getKey()));
                        return Map.entry(e.getKey(),processedValue);
                        })
                        .filter(e->e.getKey()!=null && e.getValue()!=null)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    valueMap = Map.of("schema",buildSchema(null,message),"payload",processed);
                }
                try {
                    // TODO make pretty printer configurable
                    byte[] val = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(valueMap);
                    if (debug) {
                        logger.info("to Connect value. topic: {} value {}}", topic, new String(val, StandardCharsets.UTF_8));
                    }
                    return val;
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Json issue", e);
                }
            }
        };
    }
}
