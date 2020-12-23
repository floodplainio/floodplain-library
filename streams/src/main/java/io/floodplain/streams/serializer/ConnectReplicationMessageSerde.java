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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConnectReplicationMessageSerde implements Serde<ReplicationMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectReplicationMessageSerde.class);
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
//        return new Serializer<String>() {
//            @Override
//            public byte[] serialize(String topic, String data) {
//                return data.getBytes();
//            }
//        };
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

    @Override
    public Serializer<ReplicationMessage> serializer() {

        final boolean schemaEnable = false;
        final boolean debug = false;

        return new Serializer<>() {

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                logger.info("Configuring: {}", configs);
            }

            @Override
            public byte[] serialize(String topic, ReplicationMessage replMessage) {
                if (replMessage == null || replMessage.operation() == ReplicationMessage.Operation.DELETE) {
                    return null;
                }
                Map<String, Object> valueMap = replMessage.valueMap(true, Collections.emptySet());
                if (schemaEnable) {
                    Map<String, Object> valueWithPayload = new HashMap<String, Object>();
                    valueWithPayload.put("payload", valueMap);
                    valueMap = valueWithPayload;
                }
                try {
                    byte[] val = objectMapper.writeValueAsBytes(valueMap);
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
