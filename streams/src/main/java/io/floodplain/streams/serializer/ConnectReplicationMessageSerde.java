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

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.impl.json.ReplicationJSON;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

public class ConnectReplicationMessageSerde implements Serde<ReplicationMessage> {


    private static final Logger logger = LoggerFactory.getLogger(ConnectReplicationMessageSerde.class);

    public ConnectReplicationMessageSerde() {
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Deserializer<ReplicationMessage> deserializer() {
        return new Deserializer<ReplicationMessage>() {

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> config, boolean isKey) {
                logger.info("Configuring deserializer: {}", config);

            }

            @Override
            public ReplicationMessage deserialize(String topic, byte[] data) {
                throw new UnsupportedOperationException("TODO");
            }
        };
    }

    @Override
    public Serializer<ReplicationMessage> serializer() {
        return new Serializer<ReplicationMessage>() {

            private boolean isKey;

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                logger.info("Configuring: {}", configs);
                this.isKey = isKey;
            }

            @Override
            public byte[] serialize(String topic, ReplicationMessage data) {
                if (data == null) {
                    return null;
                }
                if (isKey) {
                    return data.primaryKeys().stream().collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR)).getBytes();
                } else {
                    byte[] replicationToConnectJSON = ReplicationJSON.replicationToConnectJSON(data);
                    return replicationToConnectJSON;

                }
            }
        };
    }

}
