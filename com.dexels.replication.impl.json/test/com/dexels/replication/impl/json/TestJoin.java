package com.dexels.replication.impl.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;

public class TestJoin {

	private ReplicationMessage organizationaddress;
	private ReplicationMessage organization;

	public static void main(String[] args) {
	}

	@Before
	public void setup() {
		System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		ReplicationFactory.setInstance(parser);
		organizationaddress = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organizationaddress.json"));
		organization = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organization.json")); 
	}
	
	@Test
	public void testIgnore() {
		 System.err.println( organization.columnNames());
			Assert.assertEquals(8, organization.columnNames().size());
			System.err.println(">>>>>>>"+organization.values());
			
		int count = organization.without(Arrays.asList(new String[]{"updateby","lastupdate"})).columnNames().size();
		Assert.assertEquals(6, count);
	}
	@Test
	public void testMessageList() {
		System.err.println("Output: "+organizationaddress.primaryKeys());
		List<String> primaryValues = organizationaddress.primaryKeys().stream().map(k->organizationaddress.columnValue(k).toString()).collect(Collectors.toList());
		
		String[] parts = primaryValues.toArray(new String[0]);
		String joined = String.join("-", parts);
		Assert.assertEquals(organizationaddress.columnValue(organizationaddress.primaryKeys().get(0))+"-"+organizationaddress.columnValue(organizationaddress.primaryKeys().get(1)), joined);
		System.err.println("Joined: "+joined);
		List<ReplicationMessage> list = new ArrayList<>();
		list.add(organizationaddress);
		
	}

	@Test
	public void exploreSizes() {
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		ReplicationMessage organization = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organization.json"));
		ReplicationMessage address = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("address_lite.json"));
		List<ReplicationMessage> addresses = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			addresses.add(address);
		}
		organization = organization.withSubMessages("addresses", addresses.stream().map(r->r.message()).collect(Collectors.toList()));
		int length = organization.toBytes(parser).length;
		System.err.println("org:\n"+new String(organization.toBytes(parser)));
		System.err.println("Length: "+length);
		Assert.assertTrue(length>10000);
		
	}
	
	 public static byte[] randomByteArray(int size){
		   byte[] result= new byte[size];
		   Random random= new Random();
		   random.nextBytes(result);
		   return result;
	 }


	@Test
	public void exploreMD5Performance() throws NoSuchAlgorithmException, IOException {
		
		testHashing(200000,"SHA-256",100);
		testHashing(200000,"SHA-1",100);
		testHashing(200000,"MD5",100);
		testZip(100000,"zip",5);
	}

	private void testHashing(int size, String type, int count) throws NoSuchAlgorithmException {
		byte[] data = randomByteArray(size);
		MessageDigest dd = MessageDigest.getInstance(type);
		long now = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			dd.digest(data);
		}
		long elapsed = System.currentTimeMillis()-now;
		double rate = (double)size * ((double)count / elapsed) ;
		rate = rate / 1024;
		System.err.println("Hashing: "+count+" items with: "+type+" data size: "+data.length+" Took: "+elapsed);
		System.err.println("Rate: "+rate+" MB/s");
	}
	
	private void testZip(int size, String type, int count) throws IOException {
		byte[] data = randomByteArray(size);
		long now = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			zipBytes("something", data);
		}
		long elapsed = System.currentTimeMillis()-now;
		double rate = (double)size * ((double)count / elapsed) ;
		rate = rate / 1024;
		System.err.println("Hashing: "+count+" items with: "+type+" data size: "+data.length+" Took: "+elapsed);
		System.err.println("Rate: "+rate+" MB/s");
	}
	
	public static byte[] zipBytes(String filename, byte[] input) throws IOException {
	    ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ZipOutputStream zos = new ZipOutputStream(baos);
	    ZipEntry entry = new ZipEntry(filename);
	    entry.setSize(input.length);
	    zos.putNextEntry(entry);
	    zos.write(input);
	    zos.closeEntry();
	    zos.close();
	    return baos.toByteArray();
	}
	
	@Test
	public void parseMsg() {
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("composed.json");
		ReplicationMessage repl = parser.parseStream(stream);
		System.err.println("Names: "+repl.queueKey()+" names: "+repl.columnNames()+"\n sub: "+repl.subMessageNames()+" subli: "+repl.subMessageListNames());
	}
	
	
	@Test
	public void testMergeMessage() {
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("address.json");
		ReplicationMessage input1 = parser.parseStream(stream);

		stream = TestJoin.class.getClassLoader().getResourceAsStream("organization.json");
		ReplicationMessage input2 = parser.parseStream(stream);

		ReplicationMessage merged = input1.merge(input2, Optional.empty());
		System.err.println("FLATJSON: "+new String(merged.toBytes(parser)));
		Assert.assertNotNull(merged.columnValue("typeoforganization"));
		Assert.assertNotNull(merged.columnValue("city"));
		System.err.println(">>>>\n"+merged.columnNames().size());
		Assert.assertEquals(16, merged.columnNames().size());

//		System.err.println("Names: "+repl.queueKey()+" names: "+repl.columnNames()+"\n sub: "+repl.subMessageNames()+" subli: "+repl.subMessageListNames());
	}

	@Test
	public void testMergeMessageWithSelection() {
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("address.json");
		ReplicationMessage input1 = parser.parseStream(stream);

		stream = TestJoin.class.getClassLoader().getResourceAsStream("organization.json");
		ReplicationMessage input2 = parser.parseStream(stream);

		ReplicationMessage merged = input1.merge(input2, Optional.of(Arrays.asList(new String[]{"shortname"})));
		System.err.println("FLATJSON: "+new String(merged.toFlatString(parser)));
		Assert.assertNull(merged.columnValue("typeoforganization"));
		Assert.assertNotNull(merged.columnValue("city"));
		Assert.assertEquals(12, merged.columnNames().size());
//		System.err.println("Names: "+repl.queueKey()+" names: "+repl.columnNames()+"\n sub: "+repl.subMessageNames()+" subli: "+repl.subMessageListNames());
	}

	@Test
	public void testMessageListParser() {
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("addresslist.json");
		List<ReplicationMessage> list = parser.parseMessageList(Optional.empty(), stream);
		Assert.assertEquals(2, list.size());
	}

	@Test
	public void testToFlatMap() {
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("submessage.json");
		ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
		Map<String,Object> ss = repl.flatValueMap(true, Collections.emptySet(), "");
		System.err.println("Entry: "+ss.keySet());
		Assert.assertEquals(44, ss.size());
	}
	
	@Test
	public void testToFlatMapWithSubmessages() {
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("composed.json");
		ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);	
		Map<String,Object> ss = repl.flatValueMap(true, Collections.emptySet(), "");
		ss.entrySet().stream().forEach(e->System.err.println("Key: "+e.getKey()+" value: "+e.getValue()));
		Assert.assertEquals(388, ss.size());
	}
	
	@Test
	public void testSubmessageListAddition() {
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("composed.json");
		ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
		Assert.assertEquals(14, repl.subMessages("standings").get().size());
	
		stream = TestJoin.class.getClassLoader().getResourceAsStream("submessage.json");
		ImmutableMessage subm = ReplicationFactory.getInstance().parseStream(stream).message();
		ReplicationMessage combined = repl.withAddedSubMessage("standings", subm);
		Assert.assertEquals(15, combined.subMessages("standings").get().size());
	}
	
	@Test
	public void testSubmessageListRemoval() {
		System.err.println(System.getProperty("os.arch"));
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("composed.json");
		ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
		Assert.assertEquals(14, repl.subMessages("standings").get().size());
	
		ReplicationMessage combined = repl.withoutSubMessageInList("standings", m->((Integer)m.columnValue("homegoals"))==4);
		Assert.assertEquals(11, combined.subMessages("standings").get().size());
	}
	
	
	@Test
	public void testNull() {
		Map<String,String> types = new HashMap<>();
		Map<String,Object> values = new HashMap<>();
		types.put("Key", "integer");
		types.put("NullString", "string");
		values.put("Key", 1);
		values.put("NullString",null);
		ReplicationMessage rms = ReplicationFactory.createReplicationMessage(Optional.empty(),Optional.empty(),Optional.empty(), null, 1, Operation.INITIAL, Arrays.asList(new String[]{"Key"}), types, values, Collections.emptyMap(), Collections.emptyMap(), Optional.empty(), Optional.empty());
		System.err.println("Replication: "+rms.toFlatString(ReplicationFactory.getInstance()));
		System.err.println("Replication: "+new String(rms.toBytes(ReplicationFactory.getInstance())));
	}
	
	@Test
	public void parseParamMsg() {
		ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
		InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("organizationwithparam.json");
		ReplicationMessage repl = parser.parseStream(stream);
		Assert.assertTrue(repl.paramMessage().isPresent());
		Assert.assertEquals(12, repl.paramMessage().get().value("col2").get());
		System.err.println("Names: "+repl.queueKey()+" names: "+repl.columnNames()+"\n sub: "+repl.subMessageNames()+" subli: "+repl.subMessageListNames());
	}
	
}
