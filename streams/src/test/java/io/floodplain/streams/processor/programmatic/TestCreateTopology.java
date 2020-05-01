package io.floodplain.streams.processor.programmatic;

import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.Topology;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class TestCreateTopology {

    private static final String BROKERS = "localhost:9092";
    private final static Logger logger = LoggerFactory.getLogger(TestCreateTopology.class);


    @Test(timeout = 10000)
    @Ignore
    public void testTopology() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        Optional<AdminClient> adminClient = Optional.of(AdminClient.create(config));

        final String applicationId = "shazam-" + UUID.randomUUID().toString();
        logger.info("ApplicationId: {}", applicationId);
        Topology topology = new Topology();
        TopologyContext context = new TopologyContext(Optional.of("Generic"), "test", "my_instance", "20191214");
        TopologyConstructor topologyConstructor = new TopologyConstructor();
        ReplicationTopologyParser.addSourceStore(topology, context, topologyConstructor, Optional.empty(), "PHOTO",false);
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology);
        logger.info("{}",topology.describe().toString());


    }
}
