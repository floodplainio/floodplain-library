package com.dexels.replication.impl.protobuf.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import com.dexels.replication.impl.protobuf.generated.Replication;
import com.dexels.replication.impl.protobuf.generated.Replication.ReplicationMessageProtobuf;
import com.dexels.replication.impl.protobuf.impl.ProtobufReplicationMessageParser;
import com.google.protobuf.ByteString;


public class ReplicationProtobufTest {

	private ReplicationMessageParser protoBufParser = new ProtobufReplicationMessageParser();
	private ReplicationMessageParser jsonParser = new JSONReplicationMessageParserImpl();

	@BeforeClass
	public static void setup() {
		System.setProperty(ReplicationMessage.PRETTY_JSON,"true");
	}
	
	@Test
	public void test() {
		Map<String,Object> values = new HashMap<>();
		Map<String,String> types = new HashMap<>();
		values.put("astring", "blob");
		types.put("astring", "string");
		ReplicationMessage m =  ReplicationFactory.fromMap("key", values, types);
		m = m.withSubMessage("subb", createSubMessage());
		byte[] bb = m.toBytes(protoBufParser);
		System.err.println("Length: "+bb.length);
		ReplicationMessage rm = protoBufParser.parseBytes(Optional.empty(), bb);
		System.err.println("Astring: "+rm.columnValue("astring"));
	}

	public ImmutableMessage createSubMessage() {
		Map<String,Object> values = new HashMap<>();
		Map<String,String> types = new HashMap<>();
		values.put("bstring", "subblob");
		types.put("bstring", "string");
		return ImmutableFactory.create(values, types);
	}
	
	@Test
	public void testInteger() {
		Map<String,Object> values = new HashMap<>();
		Map<String,String> types = new HashMap<>();
		values.put("anint", 3);
		types.put("anint", "integer");
		ReplicationMessage m =  ReplicationFactory.fromMap("key", values, types);
		Object value = m.columnValue("anint");
		Assert.assertTrue(value instanceof Integer);
		byte[] bb = m.toBytes(protoBufParser);
		System.err.println("Length: "+bb.length);
		ReplicationMessage rm = protoBufParser.parseBytes(Optional.empty(),bb);
		Object value2 = rm.columnValue("anint");
		Assert.assertTrue(value2 instanceof Integer);
	}

	
	@Test
	public void testDate() throws IOException {
		try(InputStream is = ReplicationProtobufTest.class.getResourceAsStream("submessage.json")) {
			ReplicationMessage rm = jsonParser.parseStream(is);
			Date dd = (Date)rm.columnValue("formdate");
			System.err.println(">> "+dd);
		}
	}
	
	@Test
	public void testConversion() {
		try(InputStream is = ReplicationProtobufTest.class.getResourceAsStream("submessage.json")) {
			ReplicationMessage rm = jsonParser.parseStream(is);
			Assert.assertEquals(25, rm.values().size());
			byte[] bb = protoBufParser.serialize(rm);
			System.err.println("bytes: "+bb.length);
			ReplicationMessage rm2 =  protoBufParser.parseBytes(Optional.empty(),bb);
			Assert.assertEquals(25, rm2.values().size());
			byte[] bb2 = jsonParser.serialize(rm2);
			System.err.println("JSON again: "+bb2.length);
			System.err.println(">>>>\n"+new String(bb2));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testClocktime() {
		try(InputStream is = ReplicationProtobufTest.class.getResourceAsStream("calendarday.json")) {
			ReplicationMessage rm = jsonParser.parseStream(is);
			Calendar c =Calendar.getInstance();
			c.set(1970, 0, 1, 17, 0, 0);
			final Date columnValue = (Date) rm.columnValue("starttime");
			System.err.println("Comp: "+columnValue.compareTo(c.getTime()));
			System.err.println("columnValue: "+columnValue);
			System.err.println("columnValue: "+c.getTime());
			Assert.assertTrue(Math.abs(c.getTime().getTime() - columnValue.getTime())<1000);
			Assert.assertEquals(7, rm.values().size());
			byte[] bb = protoBufParser.serialize(rm);
			System.err.println("bytes: "+bb.length);
			ReplicationMessage rm2 =  protoBufParser.parseBytes(Optional.empty(),bb);
			Assert.assertEquals(7, rm2.values().size());
			
			final Date columnValue2 = (Date) rm2.columnValue("starttime");
			Assert.assertTrue(Math.abs(c.getTime().getTime() - columnValue2.getTime())<1000);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
    public void testCoordinate() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("addresslist.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);

            byte[] bb = protoBufParser.serialize(rm);

            ReplicationMessage rm2 = protoBufParser.parseBytes(Optional.empty(),bb);

            System.out.println(rm2);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
	public void testMessageListParser() {
		ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
		ReplicationMessageParser parser = new ProtobufReplicationMessageParser();
		InputStream stream = ReplicationProtobufTest.class.getResourceAsStream("addresslist.json");
		List<ReplicationMessage> list = jsonparser.parseMessageList(Optional.of("addresstopic"),stream);
		Assert.assertEquals(1, list.size());
		byte[] data = parser.serializeMessageList(list);
		System.err.println("Datasize: "+data.length);
		Assert.assertEquals(89, data.length);
		System.err.println("First: "+data[0]);
		System.err.println("Secon: "+data[1]);
		List<ReplicationMessage> list2 = parser.parseMessageList(data);
		byte[] data2 = jsonParser.serializeMessageList(list2);
		System.err.println("DATA: "+new String(data2));
	}

	@Test
	public void testMessageStreamListParser() {
		ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
		ReplicationMessageParser parser = new ProtobufReplicationMessageParser();
		InputStream stream = ReplicationProtobufTest.class.getResourceAsStream("addresslist.json");
		List<ReplicationMessage> list = jsonparser.parseMessageList(Optional.of("addresstopic"), stream);
		Assert.assertEquals(1, list.size());
		byte[] data = parser.serializeMessageList(list);
		System.err.println("Datasize: "+data.length);
		Assert.assertEquals(89, data.length);
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		List<ReplicationMessage> list2 = parser.parseMessageList(Optional.of("addresstopic"),bais);
		for (ReplicationMessage replicationMessage : list2) {
			Assert.assertFalse(replicationMessage.isErrorMessage());
		}
		System.err.println("LEN: "+list.size());
	}

	@Test
	public void testEmptyValue() {
		ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
		ReplicationMessageParser parser = new ProtobufReplicationMessageParser();
		Map<String,Object> values = new HashMap<>();
		Map<String,String> types = new HashMap<>();
		values.put("key", "bla");
		types.put("key", "string");
		values.put("empty", null);
		types.put("empty", "string");
		
		ReplicationMessage msg = ReplicationFactory.fromMap("key", values, types);
		Assert.assertNull(msg.columnValue("empty"));
//		String json = new String(msg.toBytes(jsonparser));
		byte[] protobytes = msg.toBytes(parser);
		ReplicationMessage reserialized = parser.parseBytes(Optional.empty(),protobytes);
		Assert.assertNull(reserialized.columnValue("empty"));
		String json2 = new String(reserialized.toBytes(jsonparser));
		System.err.println("json: "+json2);	
	}
	
	@Test
	public void testParseProtobuf() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("organizationwithparam.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);
            System.err.println("paramsize: "+rm.paramMessage().get().values().size());
            byte[] bb = protoBufParser.serialize(rm);
            System.err.println("# of bytes: "+bb.length);
            ReplicationMessageProtobuf rmp = Replication.ReplicationMessageProtobuf.parseFrom(bb);
            System.err.println("magic? "+rmp.getMagic());
            System.err.println("rmp: "+rmp.getParamMessage().getValuesCount());
            ReplicationMessage repl = protoBufParser.parseBytes(Optional.empty(),bb);
            System.err.println("paramsize: "+repl.paramMessage().get().values().size());
            
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	@Test
	public void testParamMessage() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("organizationwithparam.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);

            byte[] bb = protoBufParser.serialize(rm);

            ReplicationMessage repl = protoBufParser.parseBytes(Optional.empty(),bb);

    		Assert.assertTrue(repl.paramMessage().isPresent());
    		Assert.assertEquals(12, repl.paramMessage().get().value("col2").get());
    		System.err.println("Names: "+repl.queueKey()+" names: "+repl.columnNames()+"\n sub: "+repl.subMessageNames()+" subli: "+repl.subMessageListNames());


        } catch (IOException e) {
            e.printStackTrace();
        }

	}
	
	@Test
	public void testSubmessageMessage() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("submessage.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);

            byte[] bb = protoBufParser.serialize(rm);

            ReplicationMessage repl = protoBufParser.parseBytes(Optional.empty(),bb);

            
//    		Assert.assertTrue(repl.paramMessage().isPresent());
//    		Assert.assertEquals(12, repl.paramMessage().get().value("col2").get());
    		System.err.println("Names: "+repl.queueKey()+" names: "+repl.columnNames()+"\n sub: "+repl.subMessageNames()+" subli: "+repl.subMessageListNames());


        } catch (IOException e) {
            e.printStackTrace();
        }

	}
		
	@SuppressWarnings("unused")
	public void testBinaryProtobuf() {
		byte[] value = new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23};
		ByteString bs = ByteString.copyFrom((byte[])value); //parseFrom((byte[])value);
//		bs.

	}
}
