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
package io.floodplain.kafka.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ReplicationMessageConverter implements Converter {


    private boolean isKey = false;

    private boolean schemaEnable;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final static Logger logger = LoggerFactory.getLogger(ReplicationMessageConverter.class);
    private boolean debug = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());
        logger.info("Initializer of ReplicationMessageConverter key: {}", isKey);
        logger.info("Configuration: {}", configs);
        Object o = configs.get("schemas.enable");
        if (o instanceof String) {
            this.schemaEnable = Boolean.parseBoolean((String) o);
        } else if (o instanceof Boolean) {
            this.schemaEnable = Optional.of((Boolean) o).orElse(false);
        } else {
            this.schemaEnable = false;
        }
        Object debug = configs.get("debug");
        this.debug = debug!=null;
        logger.info("Debug enabled: {}",this.debug);
        this.isKey = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
//        logger.info("fromConnectData topic: {}, value: {}", topic, value);
        if (isKey) {
            if (value instanceof String) {
                String val = (String) value;
                String converted = "{\"key\": \"" + val + "\"}";
                if(debug) {
                    logger.info("Key from connect topic: {} value {}",topic, converted);
                }
                return converted.getBytes(Charset.defaultCharset());
            } else if (value instanceof Struct) {
                Struct val = (Struct) value;
                String result = schema.fields().stream().map(e -> "" + val.get(e)).collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
                if(debug) {
                    logger.info("Key struct from connect topic: {} value {}",topic, result);
                }
                return result.getBytes(Charset.defaultCharset());
            } else {
                if(debug) {
                    logger.info("Key other from connect topic: {} value {}",topic, value);
                }
                return ("" + value).getBytes(Charset.defaultCharset());
            }
        }
        ReplicationMessage rm = (ReplicationMessage) value;
        return ReplicationFactory.getInstance().serialize(rm);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {

        if (value == null) {
            if(debug) {
                logger.info("to Connect. topic: {} value null",topic);
            }
            return new SchemaAndValue(null, null);
        }
        if (isKey) {
            SchemaAndValue schemaAndValue = toConnectDataKey(value);
            if(debug) {
                logger.info("to Connect key. topic: {} value {}}",topic,schemaAndValue.value());
            }
            return schemaAndValue;
        } else {
            ReplicationMessage replMessage = ReplicationFactory.getInstance().parseBytes(Optional.empty(), value);
            Map<String, Object> valueMap = replMessage.valueMap(true, Collections.emptySet());
            if (this.schemaEnable) {
                Map<String, Object> valueWithPayload = new HashMap<String, Object>();
                valueWithPayload.put("payload", valueMap);
                valueMap = valueWithPayload;
            }
            try {
                String val = objectMapper.writeValueAsString(valueMap);
                if(debug) {
                    logger.info("to Connect value. topic: {} value {}}",topic,val);
                }
                return new SchemaAndValue(null, val);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Json issue", e);
            }
        }
    }

    private SchemaAndValue toConnectDataKey(byte[] value) {
        String valueString = new String(value, StandardCharsets.UTF_8);
        String converted = "{\"key\":\" " + valueString + "\" }";
        return new SchemaAndValue(null, converted);
    }

}
