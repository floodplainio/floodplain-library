package io.floodplain.streams.testdata;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.floodplain.replication.transformer.api.MessageTransformer;
import io.floodplain.streams.api.CoreOperators;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.base.StreamOperators;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class TestTransformations {

    private ReplicationMessage addressMessage;
    private ReplicationMessage differentAddressMessage;
    private Map<String, MessageTransformer> transformerRegistry;
    private ReplicationMessage personMessage;
    private ReplicationMessage calendarDayMessage;
    private ReplicationMessage coordinatesMessage;

    //	private ReplicationMessage playerMessage;
    private ReplicationMessage playerMessage2;
    private ReplicationMessage addressIdenticalMessage;
    private ReplicationMessage multikeys;

    @Before
    public void setUp() throws Exception {
        transformerRegistry = new HashMap<>();
        System.setProperty("PRETTY_JSON", "true");
        ReplicationMessageParser tp = new FallbackReplicationMessageParser();

        ReplicationFactory.setInstance(tp);
//        transformerRegistry.put("createcoordinatedoc", new CreateCoordinateTransformer());
//        transformerRegistry.put("formatcommunication", new CommunicationTransformer());
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
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("calendarday.json")) {
            calendarDayMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
//		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("player.json")) {
//			playerMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
//		} 
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("player2.json")) {
            playerMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("multikeys.json")) {
            multikeys = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("testsfacilitylocation.json")) {
            coordinatesMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
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

        System.err.println("Result: " + result);
//		MYTENANT-deployment-TOPICNAME		
//		MYTENANT-deployment-111-currentinstance-TOPICNAME		
//		MYTENANT-deployment-111-currentinstance-OTHERINSTANCE:TOPICNAME
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
        ReplicationMessage id = addressMessage.withPrimaryKeys(Arrays.asList(new String[]{"zipcode"}));
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
        System.err.println(">>> " + this.multikeys.queueKey());
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

    @Test
    public void mergeToListTest() {
        List<ReplicationMessage> a = new ArrayList<>();
//		sset.p
        a.add(addressMessage);
        List<ReplicationMessage> b = new ArrayList<>();
//		sset.p
        b.add(addressIdenticalMessage);
        List<ReplicationMessage> c = CoreOperators.addToReplicationList(a, b, StreamOperators.DEFAULT_MAX_LIST_SIZE, (x, y) -> x.equalsByKey(y));
        Assert.assertEquals(1, c.size());
    }

    // need to think about this one
    @Test
    public void testDeepEquality() {
        Assert.assertTrue(addressMessage.equalsToMessage(addressIdenticalMessage));
        Assert.assertFalse(addressMessage.equalsToMessage(differentAddressMessage));
    }

    @Test
    public void testJoinLists() throws IOException {
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("person.json")) {
            ReplicationMessage personMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
            List<ReplicationMessage> list = new ArrayList<>();
            list.add(personMessage);
//			list.add(playerMessage2);
            List<ReplicationMessage> list2 = new ArrayList<>();
            list2.add(personMessage2);

            List<ReplicationMessage> mm = CoreOperators.addToReplicationList(list, list2, 100, (x, y) -> x.equalsByKey(y));
            System.err.println("<>>>> " + mm.size());
            Assert.assertEquals(1, mm.size());
        }
    }


    @Test
    public void testJoinListMax() throws IOException {
        try (InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("person.json")) {
            ReplicationMessage personMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
            List<ReplicationMessage> list = new ArrayList<>();
            list.add(playerMessage2);
            List<ReplicationMessage> list2 = new ArrayList<>();
            list2.add(personMessage2);

            List<ReplicationMessage> mm = CoreOperators.addToReplicationList(list, list2, 1, (x, y) -> x.equalsByKey(y));
            System.err.println("<>>>> " + mm.size());
            Assert.assertEquals(1, mm.size());
        }
    }

}
