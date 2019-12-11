package com.dexels.kafka.streams.processor.generic;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.dexels.kafka.streams.debezium.JSONToReplicationMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class TestJsonDebeziumTransformation {
	@Test
	public void testPhoto() throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.readTree(this.getClass().getResourceAsStream("photo.json"));
		System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
		ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
		ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode)node,"photo");
		final String serialized = new String(msg.toBytes(ReplicationFactory.getInstance()));
		Assert.assertEquals(11, msg.columnNames().size());
		Assert.assertTrue(serialized.length()>20000);
		System.err.println("Message: "+serialized);

	}
}
