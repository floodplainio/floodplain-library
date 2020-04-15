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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class TestCreateTopology {

    private static final String BROKERS = "localhost:9092";


    @Test(timeout = 10000)
    @Ignore
    public void testTopology() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        Optional<AdminClient> adminClient = Optional.of(AdminClient.create(config));

        final String applicationId = "shazam-" + UUID.randomUUID().toString();
        System.err.println("ApplicationId: " + applicationId);
        Topology topology = new Topology();
        TopologyContext context = new TopologyContext(Optional.of("Generic"), "test", "my_instance", "20191214");
        TopologyConstructor topologyConstructor = new TopologyConstructor();
//		ReplicationTopologyParser.addGroupedProcessor(topology, context, topologyConstructor, name, from, ignoreOriginalKey, key, transformerSupplier);
        ReplicationTopologyParser.addSourceStore(topology, context, topologyConstructor, Optional.empty(), "PHOTO", Optional.empty(), false);
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology);
        System.err.println(topology.describe().toString());
//		KafkaStreams stream = new KafkaStreams(topology, properties);
//		stream.setUncaughtExceptionHandler((thread,exception)->logger.error("Uncaught exception from stream instance: ",exception));
//		stream.start();
//		Thread.sleep(300000);

    }
}
