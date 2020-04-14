package io.floodplain.replication.impl.protobuf.test;

import com.dexels.replication.impl.protobuf.generated.Replication;
import com.dexels.replication.impl.protobuf.generated.Replication.ReplicationMessageProtobuf;
import com.google.protobuf.ByteString;
import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.replication.impl.protobuf.impl.ProtobufReplicationMessageParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


public class ReplicationProtobufTest {

    private ReplicationMessageParser protoBufParser = new ProtobufReplicationMessageParser();
    private ReplicationMessageParser jsonParser = new JSONReplicationMessageParserImpl();

    @BeforeClass
    public static void setup() {
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
    }

    @Test
    public void test() {
        Map<String, Object> values = new HashMap<>();
        Map<String, ValueType> types = new HashMap<>();
        values.put("astring", "blob");
        types.put("astring", ValueType.STRING);
        ReplicationMessage m = ReplicationFactory.fromMap("key", values, types);
        m = m.withSubMessage("subb", createSubMessage());
        byte[] bb = m.toBytes(protoBufParser);
        System.err.println("Length: " + bb.length);
        ReplicationMessage rm = protoBufParser.parseBytes(Optional.empty(), bb);
        System.err.println("Astring: " + rm.columnValue("astring"));
    }

    public ImmutableMessage createSubMessage() {
        Map<String, Object> values = new HashMap<>();
        Map<String, ValueType> types = new HashMap<>();
        values.put("bstring", "subblob");
        types.put("bstring", ValueType.STRING);
        return ImmutableFactory.create(values, types);
    }

    @Test
    public void testInteger() {
        Map<String, Object> values = new HashMap<>();
        Map<String, ValueType> types = new HashMap<>();
        values.put("anint", 3);
        types.put("anint", ValueType.INTEGER);
        ReplicationMessage m = ReplicationFactory.fromMap("key", values, types);
        Object value = m.columnValue("anint");
        Assert.assertTrue(value instanceof Integer);
        byte[] bb = m.toBytes(protoBufParser);
        System.err.println("Length: " + bb.length);
        ReplicationMessage rm = protoBufParser.parseBytes(Optional.empty(), bb);
        Object value2 = rm.columnValue("anint");
        Assert.assertTrue(value2 instanceof Integer);
    }


    @Test
    public void testDate() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("submessage.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);
            Date dd = (Date) rm.columnValue("formdate");
            System.err.println(">> " + dd);
        }
    }

    @Test
    public void testConversion() {
        try (InputStream is = getClass().getResourceAsStream("submessage.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);
            Assert.assertEquals(25, rm.values().size());
            byte[] bb = protoBufParser.serialize(rm);
            System.err.println("bytes: " + bb.length);
            ReplicationMessage rm2 = protoBufParser.parseBytes(Optional.empty(), bb);
            Assert.assertEquals(25, rm2.values().size());
            byte[] bb2 = jsonParser.serialize(rm2);
            System.err.println("JSON again: " + bb2.length);
            System.err.println(">>>>\n" + new String(bb2));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testClocktime() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("calendarday.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);
            Calendar c = Calendar.getInstance();
            c.set(1970, 0, 1, 17, 0, 0);
            final Date columnValue = (Date) rm.columnValue("starttime");
            System.err.println("Comp: " + columnValue.compareTo(c.getTime()));
            System.err.println("columnValue: " + columnValue);
            System.err.println("columnValue: " + c.getTime());
            Assert.assertTrue(Math.abs(c.getTime().getTime() - columnValue.getTime()) < 1000);
            Assert.assertEquals(7, rm.values().size());
            byte[] bb = protoBufParser.serialize(rm);
            System.err.println("bytes: " + bb.length);
            ReplicationMessage rm2 = protoBufParser.parseBytes(Optional.empty(), bb);
            Assert.assertEquals(7, rm2.values().size());

            final Date columnValue2 = (Date) rm2.columnValue("starttime");
            Assert.assertTrue(Math.abs(c.getTime().getTime() - columnValue2.getTime()) < 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCoordinate() {
        try (InputStream is = getClass().getResourceAsStream("addresslist.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);

            byte[] bb = protoBufParser.serialize(rm);

            ReplicationMessage rm2 = protoBufParser.parseBytes(Optional.empty(), bb);

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
        List<ReplicationMessage> list = jsonparser.parseMessageList(Optional.of("addresstopic"), stream);
        Assert.assertEquals(1, list.size());
        byte[] data = parser.serializeMessageList(list);
        System.err.println("Datasize: " + data.length);
        Assert.assertEquals(89, data.length);
        System.err.println("First: " + data[0]);
        System.err.println("Secon: " + data[1]);
        List<ReplicationMessage> list2 = parser.parseMessageList(data);
        byte[] data2 = jsonParser.serializeMessageList(list2);
        System.err.println("DATA: " + new String(data2));
    }

    @Test
    public void testMessageStreamListParser() {
        ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
        ReplicationMessageParser parser = new ProtobufReplicationMessageParser();
        InputStream stream = getClass().getResourceAsStream("addresslist.json");
        List<ReplicationMessage> list = jsonparser.parseMessageList(Optional.of("addresstopic"), stream);
        Assert.assertEquals(1, list.size());
        byte[] data = parser.serializeMessageList(list);
        System.err.println("Datasize: " + data.length);
        Assert.assertEquals(89, data.length);
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        List<ReplicationMessage> list2 = parser.parseMessageList(Optional.of("addresstopic"), bais);
        for (ReplicationMessage replicationMessage : list2) {
            Assert.assertFalse(replicationMessage.isErrorMessage());
        }
        System.err.println("LEN: " + list.size());
    }

    @Test
    public void testEmptyValue() {
        ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
        ReplicationMessageParser parser = new ProtobufReplicationMessageParser();
        Map<String, Object> values = new HashMap<>();
        Map<String, ValueType> types = new HashMap<>();
        values.put("key", "bla");
        types.put("key", ValueType.STRING);
        values.put("empty", null);
        types.put("empty", ValueType.STRING);

        ReplicationMessage msg = ReplicationFactory.fromMap("key", values, types);
        Assert.assertNull(msg.columnValue("empty"));
//		String json = new String(msg.toBytes(jsonparser));
        byte[] protobytes = msg.toBytes(parser);
        ReplicationMessage reserialized = parser.parseBytes(Optional.empty(), protobytes);
        Assert.assertNull(reserialized.columnValue("empty"));
        String json2 = new String(reserialized.toBytes(jsonparser));
        System.err.println("json: " + json2);
    }

    @Test
    public void testParseProtobuf() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("organizationwithparam.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);
            System.err.println("paramsize: " + rm.paramMessage().get().values().size());
            byte[] bb = protoBufParser.serialize(rm);
            System.err.println("# of bytes: " + bb.length);
            ReplicationMessageProtobuf rmp = Replication.ReplicationMessageProtobuf.parseFrom(bb);
            System.err.println("magic? " + rmp.getMagic());
            System.err.println("rmp: " + rmp.getParamMessage().getValuesCount());
            ReplicationMessage repl = protoBufParser.parseBytes(Optional.empty(), bb);
            System.err.println("paramsize: " + repl.paramMessage().get().values().size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testParamMessage() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("organizationwithparam.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);

            byte[] bb = protoBufParser.serialize(rm);

            ReplicationMessage repl = protoBufParser.parseBytes(Optional.empty(), bb);

            Assert.assertTrue(repl.paramMessage().isPresent());
            Assert.assertEquals(12, repl.paramMessage().get().value("col2").get());
            System.err.println("Names: " + repl.queueKey() + " names: " + repl.columnNames() + "\n sub: " + repl.subMessageNames() + " subli: " + repl.subMessageListNames());


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testSubmessageMessage() {
        try (InputStream is = ReplicationProtobufTest.class.getResourceAsStream("submessage.json")) {
            ReplicationMessage rm = jsonParser.parseStream(is);

            byte[] bb = protoBufParser.serialize(rm);

            ReplicationMessage repl = protoBufParser.parseBytes(Optional.empty(), bb);


//    		Assert.assertTrue(repl.paramMessage().isPresent());
//    		Assert.assertEquals(12, repl.paramMessage().get().value("col2").get());
            System.err.println("Names: " + repl.queueKey() + " names: " + repl.columnNames() + "\n sub: " + repl.subMessageNames() + " subli: " + repl.subMessageListNames());


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @SuppressWarnings("unused")
    public void testBinaryProtobuf() {
        byte[] value = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
        ByteString bs = ByteString.copyFrom((byte[]) value); //parseFrom((byte[])value);
//		bs.

    }
}
