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

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.impl.json.ReplicationJSON;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class ImmutableMessageSerde implements Serde<ImmutableMessage> {

    public ImmutableMessageSerde() {
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Deserializer<ImmutableMessage> deserializer() {
        return new Deserializer<>() {

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> config, boolean isKey) {

            }

            @Override
            public ImmutableMessage deserialize(String topic, byte[] data) {
                try {
                    return ReplicationJSON.parseImmutable(data);
                } catch (IOException e) {
                    throw new RuntimeException("Error parsing json immutable:", e);
                }
            }
        };
    }

    @Override
    public Serializer<ImmutableMessage> serializer() {
        return new Serializer<>() {

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, ImmutableMessage data) {
                if (data == null) {
                    return null;
                }
                return ReplicationJSON.immutableTotalToJSON(data);
            }
        };
    }

}
