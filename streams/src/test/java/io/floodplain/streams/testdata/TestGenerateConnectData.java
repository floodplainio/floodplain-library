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
package io.floodplain.streams.testdata;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.streams.serializer.ConnectReplicationMessageSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

public class TestGenerateConnectData {

    private final static Logger logger = LoggerFactory.getLogger(TestGenerateConnectData.class);
    private final ReplicationMessageParser jsonParser = new JSONReplicationMessageParserImpl();

    @Test
    public void testGenerateDataWithSchema() {
            try (InputStream is = TestGenerateConnectData.class.getClassLoader().getResourceAsStream("calendarday.json")) {
                ReplicationMessage rm = jsonParser.parseStream(is);
                final LocalTime columnValue = (LocalTime) rm.value("starttime").get();
                Assert.assertEquals(48,columnValue.getMinute() );
                Assert.assertEquals(columnValue.getHour(),14 );
                Assert.assertEquals(10, rm.values().size());
                ConnectReplicationMessageSerde serde = new ConnectReplicationMessageSerde();
                Serializer<ReplicationMessage> serializer = serde.serializer();
                serializer.configure(Map.of("schemaEnable", Boolean.TRUE),false);
                byte[] data = serializer.serialize("mytopic",rm);

                logger.info("connectjson: \n" + new String(data,StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Test
    public void testBasicMessageWithDate() {
        try (InputStream is = TestGenerateConnectData.class.getClassLoader().getResourceAsStream("calendarday.json")) {
            ImmutableMessage rm = ImmutableFactory.empty().with("sometimestamp", LocalDateTime.now(), ImmutableMessage.ValueType.TIMESTAMP);

            ConnectReplicationMessageSerde serde = new ConnectReplicationMessageSerde();
            Serializer<ReplicationMessage> serializer = serde.serializer();
            serializer.configure(Map.of("schemaEnable", Boolean.TRUE),false);
            byte[] data = serializer.serialize("mytopic", ReplicationFactory.standardMessage(rm));

            logger.info("connectjson: \n" + new String(data, StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
