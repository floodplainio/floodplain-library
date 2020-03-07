package com.dexels.kafka.streams.testdata;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.StreamOperators;
import com.dexels.kafka.streams.transformer.custom.*;
import com.dexels.kafka.streams.xml.XmlMessageTransformerImpl;
import com.dexels.kafka.streams.xml.parser.CaseSensitiveXMLElement;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import com.dexels.replication.transformer.api.MessageTransformer;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class TestTransformations {

	private ReplicationMessage addressMessage;
	private ReplicationMessage differentAddressMessage;
	private Map<String,MessageTransformer> transformerRegistry;
	private XmlMessageTransformerImpl persontransform;
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
		transformerRegistry.put("formatgender", new FormatGenderTransformer());
		transformerRegistry.put("mergedatetime", new MergeDateTimeTransformer());
        transformerRegistry.put("createlist", new CreateListTransformer());
        transformerRegistry.put("createcoordinatedoc", new CreateCoordinateTransformer());
		transformerRegistry.put("formatcommunication", new CommunicationTransformer());
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("address1.json")) {
			addressMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("address2.json")) {
			differentAddressMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		}
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("address1_identical.json")) {
			addressIdenticalMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("person.json")) {
			personMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("calendarday.json")) {
		    calendarDayMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        } 
//		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("player.json")) {
//			playerMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
//		} 
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("player2.json")) {
			playerMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("multikeys.json")) {
			multikeys = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
        try (InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("testsfacilitylocation.json")) {
            coordinatesMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
        }

		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("persontransform.xml")) {
			XMLElement xe = new CaseSensitiveXMLElement();
			xe.parseFromStream(resourceAsStream);
			persontransform = new XmlMessageTransformerImpl(transformerRegistry, xe,"TENANT-topic");
		} 

	}

	@Test
	public void testTopicNameConstruction() {
		String result = CoreOperators.topicName("TOPICNAME",new TopologyContext(Optional.of("MYTENANT"), "deployment", "currentinstance", "111"));
		Assert.assertEquals("MYTENANT-deployment-TOPICNAME", result);

		result = CoreOperators.topicName("@TOPICNAME",new TopologyContext(Optional.of("MYTENANT"), "deployment", "currentinstance", "111"));
		Assert.assertEquals("MYTENANT-deployment-111-currentinstance-TOPICNAME", result);

		result = CoreOperators.topicName("@otherinstance:TOPICNAME",new TopologyContext(Optional.of("MYTENANT"), "deployment", "currentinstance", "111"));
		Assert.assertEquals("MYTENANT-deployment-111-otherinstance-TOPICNAME", result);

		result = CoreOperators.topicName("TOPICNAME",new TopologyContext(Optional.empty(), "deployment", "currentinstance", "111"));
		Assert.assertEquals("deployment-TOPICNAME", result);

		result = CoreOperators.topicName("@TOPICNAME",new TopologyContext(Optional.empty(), "deployment", "currentinstance", "111"));

		Assert.assertEquals("deployment-111-currentinstance-TOPICNAME", result);

		result = CoreOperators.topicName("@otherinstance:TOPICNAME",new TopologyContext(Optional.empty(), "deployment", "currentinstance", "111"));
		Assert.assertEquals("deployment-111-otherinstance-TOPICNAME", result);


		
		System.err.println("Result: "+result);
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
		Assert.assertNull( addressMessage.without("zipcode").columnValue("zipcode"));
	}

	@Test
	public void testAdd() {
		Assert.assertEquals("monkey", addressMessage.with("animal","monkey","string").columnValue("animal"));
		Assert.assertEquals("string", addressMessage.with("animal","monkey","string").columnType("animal"));
	}
	
	@Test
	public void testTransformerRename() {
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><rename from=\"zipcode\" to=\"zippy\"/></transform>");
			XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
			ReplicationMessage result = xmt.apply(null, addressMessage);
			Assert.assertEquals("4565AB", result.columnValue("zippy"));
	}
	@Test
	public void testTransformerRenameNull() {
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><rename from=\"notexisting\" to=\"zippy\"/></transform>");
			XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
			ReplicationMessage result = xmt.apply(null, addressMessage);
			Assert.assertNull( result.columnValue("zippy"));
	}
	   
    @Test @Ignore
    // TODO Figure out why this test fails @CircleCI
    public void testMergeDateTime_Clocktime() {
        XMLElement xe = new CaseSensitiveXMLElement();
        xe.parseString("<transform><mergedatetime fromdate=\"calendardate\" fromtime=\"starttime\" to=\"datetime\" /></transform>");
        XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
        ReplicationMessage result = xmt.apply(null, calendarDayMessage);
        Assert.assertNotNull(result.columnValue("datetime"));
        Assert.assertEquals(Date.class, result.columnValue("datetime").getClass());
        Assert.assertEquals(1462452480000L, ((Date) result.columnValue("datetime")).getTime());
    }
    
    @Test @Ignore
    // TODO Figure out why this test fails @CircleCI
    public void testMergeDateTime_Date() {
        XMLElement xe = new CaseSensitiveXMLElement();
        xe.parseString("<transform><mergedatetime fromdate=\"calendardate\" fromtime=\"starttimedate\" to=\"datetime\" /></transform>");
        XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
        ReplicationMessage result = xmt.apply(null, calendarDayMessage);
        Assert.assertNotNull(result.columnValue("datetime"));
        Assert.assertEquals(Date.class, result.columnValue("datetime").getClass());
        Assert.assertEquals(1462452480000L, ((Date) result.columnValue("datetime")).getTime());
    }

    @Test @Ignore
    public void testCoordinatesTransformerFromList() {
        XMLElement xe1 = new CaseSensitiveXMLElement();
        xe1.parseString("<transform><createlist from=\"longitude,latitude\" to=\"myCoordinate\" /></transform>");
        XmlMessageTransformerImpl xmt1 = new XmlMessageTransformerImpl(transformerRegistry, xe1, "TENANT-topic");
        ReplicationMessage result1 = xmt1.apply(null, coordinatesMessage);

        XMLElement xe = new CaseSensitiveXMLElement();
        xe.parseString(
                "<transform><createcoordinatedoc from=\"myCoordinate\" to=\"myCoordinateDoc\" /></transform>");
        XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry, xe, "TENANT-topic");
        ReplicationMessage result = xmt.apply(null, result1);
        Assert.assertNotNull(result.columnValue("myCoordinateDoc"));
        Assert.assertNotNull(result.columnValue("myCoordinateDoc"));
        Assert.assertEquals("coordinate", Document.parse((String) result.columnValue("myCoordinateDoc")).get("_type"));
        Assert.assertEquals("Point", Document.parse((String) result.columnValue("myCoordinateDoc")).get("type"));
        Assert.assertEquals("coordinate", Document.parse((String) result.columnValue("myCoordinateDoc")).get("_type"));
        Assert.assertEquals(ArrayList.class, Document.parse((String) result.columnValue("myCoordinateDoc")).get("coordinates").getClass());
        Assert.assertEquals("[-50.0, 13.0]", Document.parse((String) result.columnValue("myCoordinateDoc")).get("coordinates").toString());
    }

    @Test @Ignore
    public void testCoordinatesTransformerFromFields() {
        XMLElement xe1 = new CaseSensitiveXMLElement();
        xe1.parseString("<transform><createcoordinatedoc from=\"longitude,latitude\" to=\"myCoordinateDoc\" /></transform>");
        XmlMessageTransformerImpl xmt1 = new XmlMessageTransformerImpl(transformerRegistry, xe1, "TENANT-topic");
        ReplicationMessage result = xmt1.apply(null, coordinatesMessage);
        Assert.assertNotNull(result.columnValue("myCoordinateDoc"));
        Assert.assertEquals("coordinate", Document.parse((String) result.columnValue("myCoordinateDoc")).get("_type"));
        Assert.assertEquals("Point", Document.parse((String) result.columnValue("myCoordinateDoc")).get("type"));
        Assert.assertEquals("coordinate", Document.parse((String) result.columnValue("myCoordinateDoc")).get("_type"));
        Assert.assertEquals(ArrayList.class, Document.parse((String) result.columnValue("myCoordinateDoc")).get("coordinates").getClass());
        Assert.assertEquals("[-50.0, 13.0]", Document.parse((String) result.columnValue("myCoordinateDoc")).get("coordinates").toString());
    }

    @Test @Ignore
    public void testListTransformer() {
        XMLElement xe = new CaseSensitiveXMLElement();
        xe.parseString("<transform><createlist from=\"longitude,latitude\" to=\"myCoordinate\" /></transform>");
        XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry, xe, "TENANT-topic");
        ReplicationMessage result = xmt.apply(null, coordinatesMessage);

        Assert.assertNotNull(result.columnValue("myCoordinate"));
        Assert.assertEquals(ArrayList.class, result.columnValue("myCoordinate").getClass());
        Assert.assertEquals("[-50.0, 13.0]", result.columnValue("myCoordinate").toString());
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
	public void testTransformerRenameSubMessage() {
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><renameSubMessages from=\"communication\" to=\"Communication\"/></transform>");
			XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
			ReplicationMessage result = xmt.apply(null, playerMessage2);
			System.err.println("MMMMSG:\n"+new String(result.toBytes(ReplicationFactory.getInstance())));
			Assert.assertTrue(result.subMessages("Communication").isPresent());
			Assert.assertFalse(result.subMessages("communication").isPresent());
	}
	

	@Test
	public void testTransformerRemove() {
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><remove name=\"streetname\"/></transform>");
			XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
			ReplicationMessage result = xmt.apply(null, addressMessage);
			Assert.assertNull(result.columnValue("streetname"));
	}

	@Test
	public void testTransformerRemoveNull() {
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><remove name=\"notexisting\"/></transform>");
			XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
			ReplicationMessage result = xmt.apply(null, addressMessage);
			Assert.assertNull(result.columnValue("notexisting"));
	}
	
	@Test
	public void testTransformerApply() {
		transformerRegistry.put("streettoupper", (params,msg)->msg.with("streetname", ((String)(msg.columnValue("streetname"))).toString().toUpperCase(),"string"));
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><streettoupper/></transform>");
		XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
		ReplicationMessage result = xmt.apply(null, addressMessage);
		Assert.assertEquals("KONIJNENBERGWEG", result.columnValue("streetname"));
	}
	
	@Test
	public void testTransformerApplyWithNull() {
		transformerRegistry.put("streettoupper", (params,msg)->msg.with("streetname", ((String)(msg.columnValue("streetname"))).toString().toUpperCase(),"string"));
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><streettoupper/></transform>");
		XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
		ReplicationMessage result = xmt.apply(null, addressMessage);
		Assert.assertNull(result.columnValue("notexisting"));
	}

	@Test
	public void testTransformerApplyWithParams() {
		transformerRegistry.put("fieldtoupper", 
				(params,msg)->
					msg.with(params.get("field"), 
							((String)(msg.columnValue(params.get("field"))))
							.toString()
							.toUpperCase(),"string"));
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><fieldtoupper field=\"streetname\"/></transform>");
		XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
		ReplicationMessage result = xmt.apply(Collections.emptyMap(), addressMessage);
		Assert.assertEquals("KONIJNENBERGWEG", result.columnValue("streetname"));
	}
	@Test
	public void testTransformerApplyWithParamsWithMissingField() {
		transformerRegistry.put("fieldtoupper", (params,msg)->{
			String fieldValue = (String)(msg.columnValue(params.get("field")));
			if(fieldValue==null) {
				return msg;
			}
			return msg.with(params.get("field"), fieldValue.toString().toUpperCase(),"string");
		});
		XMLElement xe = new CaseSensitiveXMLElement();
		xe.parseString("<transform><fieldtoupper field=\"notexisting\"/></transform>");
		XmlMessageTransformerImpl xmt = new XmlMessageTransformerImpl(transformerRegistry,xe,"TENANT-topic");
		ReplicationMessage result = xmt.apply(null, addressMessage);
		Assert.assertEquals("Konijnenbergweg", result.columnValue("streetname"));
	}

	@Test
	public void testPersonTransformer() {
		ReplicationMessage result =  this.persontransform.apply(null, this.playerMessage2);
		System.err.println("Result:\n"+result.toFlatString(ReplicationFactory.getInstance()));
		Optional<ImmutableMessage> rm = result.subMessage("Communication");
		Assert.assertTrue(rm.isPresent());
		String email = (String) rm.get().columnValue("Email");
		Assert.assertNotNull(email);
	}

	
	@Test
	public void testEqual() {
		Assert.assertTrue(addressMessage.equals(addressIdenticalMessage));
		Assert.assertTrue(addressIdenticalMessage.equals(addressMessage));
	}

	@Test
	public void testMultikeys() {
		System.err.println(">>> "+this.multikeys.queueKey());
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
		List<ReplicationMessage> c = CoreOperators.addToReplicationList(a, b,StreamOperators.DEFAULT_MAX_LIST_SIZE,(x,y)->x.equalsByKey(y));
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
		try(InputStream resourceAsStream = TestTransformations.class.getClassLoader().getResourceAsStream("person.json")) {
			ReplicationMessage personMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
			List<ReplicationMessage> list = new ArrayList<>();
			list.add(personMessage);
//			list.add(playerMessage2);
			List<ReplicationMessage> list2 = new ArrayList<>();
			list2.add(personMessage2);

			List<ReplicationMessage> mm = CoreOperators.addToReplicationList(list, list2,100,(x,y)->x.equalsByKey(y));
			System.err.println("<>>>> "+mm.size());
			Assert.assertEquals(1, mm.size());
		}
	}



	@Test
	public void testJoinListMax() throws IOException {
		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("person.json")) {
			ReplicationMessage personMessage2 = ReplicationFactory.getInstance().parseStream(resourceAsStream);
			List<ReplicationMessage> list = new ArrayList<>();
			list.add(playerMessage2);
			List<ReplicationMessage> list2 = new ArrayList<>();
			list2.add(personMessage2);

			List<ReplicationMessage> mm = CoreOperators.addToReplicationList(list, list2,1,(x,y)->x.equalsByKey(y));
			System.err.println("<>>>> "+mm.size());
			Assert.assertEquals(1, mm.size());
		}
	}

}
