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
package io.floodplain.replication.impl.json.test;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.replication.impl.json.ReplicationJSON;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class TestJoin {

    private static final Logger logger = LoggerFactory.getLogger(TestJoin.class);
    private ReplicationMessage organizationaddress;
    private ReplicationMessage organization;

    @Before
    public void setup() {
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
        ReplicationFactory.setInstance(parser);
        organizationaddress = parser.parseStream(getClass().getClassLoader().getResourceAsStream("organizationaddress.json"));
        organization = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organization.json"));
    }

    @Test
    public void testIgnore() {
        logger.info("{}",organization.columnNames());
        Assert.assertEquals(8, organization.columnNames().size());
        logger.info(">>>>>>> {}",organization.values());

        int count = organization.without(Arrays.asList("updateby", "lastupdate")).columnNames().size();
        Assert.assertEquals(6, count);
    }

    @Test
    public void testMessageList() {
        logger.info("Output: " + organizationaddress.primaryKeys());
        List<String> primaryValues = organizationaddress.primaryKeys().stream()
                .map(k -> organizationaddress.columnValue(k).toString()).collect(Collectors.toList());

        String[] parts = primaryValues.toArray(new String[0]);
        String joined = String.join("-", parts);
        Assert.assertEquals(organizationaddress.columnValue(organizationaddress.primaryKeys().get(0)) + "-"
                + organizationaddress.columnValue(organizationaddress.primaryKeys().get(1)), joined);
        logger.info("Joined: " + joined);
        List<ReplicationMessage> list = new ArrayList<>();
        list.add(organizationaddress);

    }

    @Test
    public void exploreSizes() {
        ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
        ReplicationMessage organization = parser
                .parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organization.json"));
        ReplicationMessage address = parser
                .parseStream(TestJoin.class.getClassLoader().getResourceAsStream("address_lite.json"));
        List<ReplicationMessage> addresses = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            addresses.add(address);
        }
        organization = organization.withSubMessages("addresses",
                addresses.stream().map(ReplicationMessage::message).collect(Collectors.toList()));
        int length = organization.toBytes(parser).length;
        logger.info("org:\n" + new String(organization.toBytes(parser),StandardCharsets.UTF_8));
        logger.info("Length: " + length);
        Assert.assertTrue(length > 10000);

    }

    public static byte[] randomByteArray(int size) {
        byte[] result = new byte[size];
        Random random = new Random();
        random.nextBytes(result);
        return result;
    }

    @Test
    public void exploreMD5Performance() throws NoSuchAlgorithmException, IOException {

        testHashing(200000, "SHA-256", 100);
        testHashing(200000, "SHA-1", 100);
        testHashing(200000, "MD5", 100);
        testZip(100000, "zip", 5);
    }

    private void testHashing(int size, String type, int count) throws NoSuchAlgorithmException {
        byte[] data = randomByteArray(size);
        MessageDigest dd = MessageDigest.getInstance(type);
        long now = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            dd.digest(data);
        }
        long elapsed = System.currentTimeMillis() - now;
        double rate = (double) size * ((double) count / elapsed);
        rate = rate / 1024;
        logger.info(
                "Hashing: " + count + " items with: " + type + " data size: " + data.length + " Took: " + elapsed);
        logger.info("Rate: " + rate + " MB/s");
    }

    private void testZip(int size, String type, int count) throws IOException {
        byte[] data = randomByteArray(size);
        long now = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            zipBytes("something", data);
        }
        long elapsed = System.currentTimeMillis() - now;
        double rate = (double) size * ((double) count / elapsed);
        rate = rate / 1024;
        logger.info(
                "Hashing: " + count + " items with: " + type + " data size: " + data.length + " Took: " + elapsed);
        logger.info("Rate: " + rate + " MB/s");
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
        logger.info("Names: " + repl.queueKey() + " names: " + repl.columnNames() + "\n sub: "
                + repl.message().subMessageNames() + " subli: " + repl.subMessageListNames());
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
        Map<String, Object> ss = repl.flatValueMap(true, Collections.emptySet(), "");
        logger.info("Entry: " + ss.keySet());
        Assert.assertEquals(44, ss.size());
    }

    @Test
    public void testToFlatMapWithParam() {
        InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("submessage.json");
        ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
        ReplicationMessage replWithParam = repl.withParamMessage(ImmutableFactory.empty().with("key", "value", ImmutableMessage.ValueType.STRING));
        Map<String, Object> ss = replWithParam.flatValueMap(true, Collections.emptySet(), "");
        logger.info("Entry: " + ss.keySet());
        Assert.assertEquals(45, ss.size());
    }

    @Test
    public void testToFlatMapWithSubmessages() {
        InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("composed.json");
        ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
        Map<String, Object> ss = repl.flatValueMap(true, Collections.emptySet(), "");
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
        logger.info(System.getProperty("os.arch"));
        InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("composed.json");
        ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
        Assert.assertEquals(14, repl.subMessages("standings").get().size());

        ReplicationMessage combined = repl.withoutSubMessageInList("standings",
                m -> ((Integer) m.columnValue("homegoals")) == 4);
        Assert.assertEquals(11, combined.subMessages("standings").get().size());
    }

    @Test
    public void testToConnectStyleJSON() {
        InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("organizationaddress.json");
        ReplicationMessage repl = ReplicationFactory.getInstance().parseStream(stream);
        byte[] data = ReplicationJSON.replicationToConnectJSON(repl);
        logger.info(">>>>|>>>\n" + new String(data, StandardCharsets.UTF_8));
    }

    @Test
    public void testNull() {
        Map<String, ImmutableMessage.ValueType> types = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        types.put("Key", ImmutableMessage.ValueType.INTEGER);
        types.put("NullString", ImmutableMessage.ValueType.STRING);
        values.put("Key", 1);
        values.put("NullString", null);
        ReplicationMessage rms = ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(),
                Optional.empty(), null, 1, ReplicationMessage.Operation.UPDATE, Arrays.asList("Key"), types, values,
                Collections.emptyMap(), Collections.emptyMap(), Optional.empty(), Optional.empty());
        logger.info("Replication: " + rms.toFlatString(ReplicationFactory.getInstance()));
        logger.info("Replication: " + new String(rms.toBytes(ReplicationFactory.getInstance()), StandardCharsets.UTF_8));
    }

    @Test
    public void parseParamMsg() {
        ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
        InputStream stream = TestJoin.class.getClassLoader().getResourceAsStream("organizationwithparam.json");
        ReplicationMessage repl = parser.parseStream(stream);
        Assert.assertTrue(repl.paramMessage().isPresent());
        Assert.assertEquals(12, repl.paramMessage().get().value("col2").get());
        logger.info("Names: " + repl.queueKey() + " names: " + repl.columnNames() + "\n sub: "
                + repl.message().subMessageNames() + " subli: " + repl.subMessageListNames());
    }

}
