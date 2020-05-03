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
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReplicationMessageListSerde implements Serde<List<ReplicationMessage>> {

    private final FallbackReplicationMessageParser parser = new FallbackReplicationMessageParser();
    private static final Logger logger = LoggerFactory.getLogger(ReplicationMessageListSerde.class);

    public ReplicationMessageListSerde() {
        ReplicationFactory.setInstance(parser);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Configuring: {}", configs);
    }

    @Override
    public Deserializer<List<ReplicationMessage>> deserializer() {
        return new Deserializer<List<ReplicationMessage>>() {

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> config, boolean isKey) {

            }

            @Override
            public List<ReplicationMessage> deserialize(String topic, byte[] data) {
                if (data == null) {
                    return Collections.emptyList();
                }
                return parser.parseMessageList(data);
            }
        };
    }

    @Override
    public Serializer<List<ReplicationMessage>> serializer() {
        return new Serializer<List<ReplicationMessage>>() {

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {

            }

            @Override
            public byte[] serialize(String topic, List<ReplicationMessage> data) {

                return parser.serializeMessageList(data);


            }
        };
    }

}
