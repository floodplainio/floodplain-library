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
package io.floodplain.replication.impl.protobuf.test;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class TestSer {

    @Test
    public void testBinaryJSON() {
        final ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
        byte[] payload = "123".getBytes(StandardCharsets.UTF_8);
        final byte[] deserialized = testSerialization(parser, payload);
        Assert.assertArrayEquals(payload, deserialized);
    }

    @Test
    public void testBinaryProtobuf() {
        final ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
        byte[] payload = "123".getBytes(StandardCharsets.UTF_8);
        final byte[] deserialized = testSerialization(parser, payload);
        Assert.assertArrayEquals(payload, deserialized);
    }

    private byte[] testSerialization(final ReplicationMessageParser parser, byte[] payload) {
        ReplicationMessage r = ReplicationFactory.empty().with("binary", payload, ImmutableMessage.ValueType.BINARY);
        byte[] encoded = r.toBytes(parser);
        ReplicationMessage s = parser.parseBytes(Optional.of("binary"), encoded);
        return (byte[]) s.value("binary").get();
    }


}
