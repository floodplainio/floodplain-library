package io.floodplain.kafka.webapi;

import io.floodplain.streams.api.CoreOperators;
import io.floodplain.streams.api.TopologyContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;


public class TestResolveGeneration {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() {
        String s = "@replication";
        String resolvedDatabaseName = CoreOperators.topicName(s, new TopologyContext(Optional.of("TENANT"), "deployment", "total", "generation-12345"));

        assertEquals("TENANT-deployment-generation-12345-total-replication", resolvedDatabaseName);

    }

}
