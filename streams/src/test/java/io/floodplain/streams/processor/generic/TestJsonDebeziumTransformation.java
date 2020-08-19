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
package io.floodplain.streams.processor.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;


public class TestJsonDebeziumTransformation {

    private static final Logger logger = LoggerFactory.getLogger(TestJsonDebeziumTransformation.class);

    @Test
    public void testPhoto() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("photo.json"));
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, Optional.of("photo"));
        final String serialized = new String(msg.toBytes(ReplicationFactory.getInstance()), StandardCharsets.UTF_8);
        Assert.assertEquals(11, msg.columnNames().size());
        Assert.assertTrue(serialized.length() > 20000);

    }

    @Test
    public void testDecimal() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("decimalwithscale.json"));
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, Optional.of("photo"));
        final String serialized = new String(msg.toBytes(ReplicationFactory.getInstance()), StandardCharsets.UTF_8);
        logger.info("serialized: {}",serialized);
    }
}
