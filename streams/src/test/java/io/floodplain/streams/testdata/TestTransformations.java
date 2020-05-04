package io.floodplain.streams.testdata;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.floodplain.streams.api.CoreOperators;
import io.floodplain.streams.api.TopologyContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;


public class TestTransformations {

    private ReplicationMessage addressMessage;
    private ReplicationMessage differentAddressMessage;
    private ReplicationMessage personMessage;

    private ReplicationMessage playerMessage2;
    private ReplicationMessage addressIdenticalMessage;
    private ReplicationMessage multikeys;

    private final static Logger logger = LoggerFactory.getLogger(TestTransformations.class);

    @Before
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
        String result = CoreOperators.topicName("TOPICNAME", new TopologyContext(Optional.of("MYTENANT"), "deployment", "currentinstance", "111"));
        Assert.assertEquals("MYTENANT-deployment-TOPICNAME", result);

        result = CoreOperators.topicName("@TOPICNAME", new TopologyContext(Optional.of("MYTENANT"), "deployment", "currentinstance", "111"));
        Assert.assertEquals("MYTENANT-deployment-111-currentinstance-TOPICNAME", result);

        result = CoreOperators.topicName("TOPICNAME", new TopologyContext(Optional.empty(), "deployment", "currentinstance", "111"));
        Assert.assertEquals("deployment-TOPICNAME", result);

        result = CoreOperators.topicName("@TOPICNAME", new TopologyContext(Optional.empty(), "deployment", "currentinstance", "111"));

        Assert.assertEquals("deployment-111-currentinstance-TOPICNAME", result);

        logger.info("Result: {}",result);
    }

    @Test
    public void test() {
        Assert.assertEquals("4565AB", addressMessage.columnValue("zipcode"));
    }

    @Test
    public void testRename() {
        Assert.assertEquals("4565AB", addressMessage.rename("zipcode", "zippy").columnValue("zippy"));
    }

    @Test
    public void testRenameKey() {
        ReplicationMessage id = addressMessage.rename("addressid", "id");
        Assert.assertEquals(1, id.primaryKeys().size());
        Assert.assertEquals("id", id.primaryKeys().stream().findFirst().get());
    }

    @Test
    public void testRemoveKey() {
        ReplicationMessage id = addressMessage.without("addressid");
        Assert.assertEquals(0, id.primaryKeys().size());
    }

    @Test
    public void testSpecificKey() {
        ReplicationMessage id = addressMessage.withPrimaryKeys(Arrays.asList("zipcode"));
        Assert.assertEquals(1, id.primaryKeys().size());
        Assert.assertEquals("zipcode", id.primaryKeys().stream().findFirst().get());
        Assert.assertEquals(id.columnValue("zipcode"), id.queueKey());
    }


    @Test
    public void testRemove() {
        Assert.assertNull(addressMessage.without("zipcode").columnValue("zipcode"));
    }

    @Test
    public void testAdd() {
        Assert.assertEquals("monkey", addressMessage.with("animal", "monkey", ImmutableMessage.ValueType.STRING).columnValue("animal"));
        Assert.assertEquals(ImmutableMessage.ValueType.STRING, addressMessage.with("animal", "monkey", ImmutableMessage.ValueType.STRING).columnType("animal"));
    }


    @Test
    public void testClearTransformerRenameSubMessages() {
        ReplicationMessage result = playerMessage2.withoutSubMessages("communication");
        Assert.assertFalse(result.subMessages("communication").isPresent());
    }

    @Test
    public void testClearTransformerRenameSubMessage() {
        Assert.assertTrue(personMessage.subMessage("fakesubmessage").isPresent());
        ReplicationMessage result = personMessage.withoutSubMessage("fakesubmessage");
        Assert.assertFalse(result.subMessage("fakesubmessage").isPresent());
    }

    @Test
    public void testEqual() {
        Assert.assertTrue(addressMessage.equals(addressIdenticalMessage));
        Assert.assertTrue(addressIdenticalMessage.equals(addressMessage));
    }

    @Test
    public void testMultikeys() {
        logger.info(">>> " + this.multikeys.queueKey());
        Assert.assertEquals("123456<$>234567<$>345678", this.multikeys.queueKey());
    }

    @Test
    public void testSet() {
        Set<ReplicationMessage> sset = new HashSet<>();
//		sset.p
        sset.add(addressMessage);
        sset.add(addressIdenticalMessage);
        Assert.assertEquals(1, sset.size());
    }

    @Test
    public void testList() {
        List<ReplicationMessage> list = new ArrayList<>();
//		sset.p
        list.add(addressMessage);
        list.add(addressIdenticalMessage);
        Assert.assertEquals(2, list.size());
    }

    // need to think about this one
    @Test
    public void testDeepEquality() {
        Assert.assertTrue(addressMessage.equalsToMessage(addressIdenticalMessage));
        Assert.assertFalse(addressMessage.equalsToMessage(differentAddressMessage));
    }




}
