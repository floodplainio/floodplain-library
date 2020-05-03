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
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ReplicationMessageDeserializer implements org.apache.kafka.common.serialization.Deserializer<ReplicationMessage> {
    private Deserializer<ReplicationMessage> deserializer = new ReplicationMessageSerde().deserializer();

    @Override
    public void close() {
        deserializer.close();

    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        deserializer.configure(arg0, arg1);

    }

    @Override
    public ReplicationMessage deserialize(String arg0, byte[] arg1) {
        return deserializer.deserialize(arg0, arg1);
    }

}
