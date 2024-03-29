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
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

public class TestBigDecimal {

    private static final Logger logger = LoggerFactory.getLogger(TestBigDecimal.class);

    @Test
    public void testBigDecimal() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("decimalwithscale.json"));
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, Optional.empty());
        Object amount = msg.value("amount").get();
        BigDecimal value = (BigDecimal) amount;
        logger.info("Final value: "+value);
    }

    @Test
    public void testDateTimestamp() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("timestamp.json"));
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, Optional.empty());
        Object amount = msg.value("created_on").get();
        LocalDateTime value = (LocalDateTime) amount;
        Assertions.assertEquals(7, value.getMonth().getValue());
        logger.info("Final value: "+value);
    }
}
