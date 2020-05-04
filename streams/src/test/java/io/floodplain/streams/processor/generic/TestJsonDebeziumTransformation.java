package io.floodplain.streams.processor.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class TestJsonDebeziumTransformation {
    @Test
    public void testPhoto() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("photo.json"));
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, "photo");
        final String serialized = new String(msg.toBytes(ReplicationFactory.getInstance()));
        Assert.assertEquals(11, msg.columnNames().size());
        Assert.assertTrue(serialized.length() > 20000);

    }

    @Test
    public void testDecimal() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("decimalwithscale.json"));
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, "photo");
        final String serialized = new String(msg.toBytes(ReplicationFactory.getInstance()));

    }
}
