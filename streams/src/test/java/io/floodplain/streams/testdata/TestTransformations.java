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
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.floodplain.streams.api.TopologyContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTransformations {

    private ReplicationMessage addressMessage;
    private ReplicationMessage differentAddressMessage;
    private ReplicationMessage personMessage;

    private ReplicationMessage playerMessage2;
    private ReplicationMessage addressIdenticalMessage;
    private ReplicationMessage multikeys;

    private final static Logger logger = LoggerFactory.getLogger(TestTransformations.class);

    @BeforeAll
    public void setUp() throws Exception {
        System.setProperty("PRETTY_JSON", "true");
        ReplicationMessageParser tp = new FallbackReplicationMessageParser();

        ReplicationFactory.setInstance(tp);
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("address1.json")) {
            addressMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("address2.json")) {
            differentAddressMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("address1_identical.json")) {
            addressIdenticalMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("person.json")) {
            personMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("player2.json")) {
            playerMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("multikeys.json")) {
            multikeys = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }


    }

    @Test
    public void testTopicNameConstruction() {
        final TopologyContext topologyContext = TopologyContext.context(Optional.of("MYTENANT"), "111");
        String result = topologyContext.topicName("TOPICNAME");
        Assertions.assertEquals("MYTENANT-TOPICNAME", result);

        result = topologyContext.topicName("@TOPICNAME");
        Assertions.assertEquals("MYTENANT-111-TOPICNAME", result);

        TopologyContext topologyContextWithoutTenant = TopologyContext.context(Optional.empty(), "111");
        result = topologyContextWithoutTenant.topicName("TOPICNAME");
        Assertions.assertEquals("TOPICNAME", result);

        result = topologyContextWithoutTenant.topicName("@TOPICNAME");

        Assertions.assertEquals("111-TOPICNAME", result);


        logger.info("Result: {}", result);
    }

    @Test
    public void test() {
        Assertions.assertEquals("4565AB", addressMessage.value("zipcode").get());
    }

    @Test
    public void testRename() {
        Assertions.assertEquals("4565AB", addressMessage.rename("zipcode", "zippy").value("zippy").get());
    }

    @Test
    public void testRenameKey() {
        ReplicationMessage id = addressMessage.rename("addressid", "id");
        Assertions.assertEquals(1, id.primaryKeys().size());
        Assertions.assertEquals("id", id.primaryKeys().stream().findFirst().get());
    }

    @Test
    public void testRemoveKey() {
        ReplicationMessage id = addressMessage.without("addressid");
        Assertions.assertEquals(0, id.primaryKeys().size());
    }

    @Test
    public void testSpecificKey() {
        ReplicationMessage id = addressMessage.withPrimaryKeys(Collections.singletonList("zipcode"));
        Assertions.assertEquals(1, id.primaryKeys().size());
        Assertions.assertEquals("zipcode", id.primaryKeys().stream().findFirst().get());
        Assertions.assertEquals(id.value("zipcode").get(), id.queueKey());
    }


    @Test
    public void testRemove() {
        Assertions.assertTrue(addressMessage.without("zipcode").value("zipcode").isEmpty());
    }

    @Test
    public void testAdd() {
        Assertions.assertEquals("monkey", addressMessage.with("animal", "monkey", ImmutableMessage.ValueType.STRING).value("animal").get());
        Assertions.assertEquals(ImmutableMessage.ValueType.STRING, addressMessage.with("animal", "monkey", ImmutableMessage.ValueType.STRING).columnType("animal"));
    }


    @Test
    public void testClearTransformerRenameSubMessages() {
        ReplicationMessage result = playerMessage2.withoutSubMessages("communication");
        Assertions.assertFalse(result.subMessages("communication").isPresent());
    }

    @Test
    public void testClearTransformerRenameSubMessage() {
        Assertions.assertTrue(personMessage.subMessage("fakesubmessage").isPresent());
        ReplicationMessage result = personMessage.withoutSubMessage("fakesubmessage");
        Assertions.assertFalse(result.subMessage("fakesubmessage").isPresent());
    }

    @Test
    public void testEqual() {
        Assertions.assertEquals(addressMessage, addressIdenticalMessage);
        Assertions.assertEquals(addressIdenticalMessage, addressMessage);
    }

    @Test
    public void testMultikeys() {
        logger.info(">>> " + this.multikeys.queueKey());
        Assertions.assertEquals("123456<$>234567<$>345678", this.multikeys.queueKey());
    }

    @Test
    public void testSet() {
        Set<ReplicationMessage> sset = new HashSet<>();
//		sset.p
        sset.add(addressMessage);
        sset.add(addressIdenticalMessage);
        Assertions.assertEquals(1, sset.size());
    }

    @Test
    public void testList() {
        List<ReplicationMessage> list = new ArrayList<>();
//		sset.p
        list.add(addressMessage);
        list.add(addressIdenticalMessage);
        Assertions.assertEquals(2, list.size());
    }

    // need to think about this one
    @Test
    public void testDeepEquality() {
        Assertions.assertTrue(addressMessage.equalsToMessage(addressIdenticalMessage));
        Assertions.assertFalse(addressMessage.equalsToMessage(differentAddressMessage));
    }


}
