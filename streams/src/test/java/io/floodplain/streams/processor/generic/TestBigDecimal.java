package io.floodplain.streams.processor.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;

public class TestBigDecimal {

    @Test
    public void testBigDecimal() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(this.getClass().getClassLoader().getResourceAsStream("decimalwithscale.json"));
        ReplicationMessage msg = JSONToReplicationMessage.convertToReplication(false, (ObjectNode) node, Optional.empty());
        Object amount = msg.columnValue("amount");
        Assert.assertTrue(amount instanceof BigDecimal);
    }
}
