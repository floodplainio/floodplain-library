package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopologyConstructor {


    //    public final Map<String, List<ConnectorTopicTuple>> connectorAssociations = new HashMap<>();
    public final Map<String, List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ImmutableMessage>>> immutableStoreSupplier = new HashMap<>();
    // TODO: Could be optional, only needed in xml based stream code
    public final Set<String> stores = new HashSet<>();
    public final Map<String, String> sources = new HashMap<>();
    // TODO could race conditions happen? If so, would that be a problem?

    private final Map<String, Optional<Integer>> desiredTopics = new HashMap<>();
    private int pipeCounter = 1;

    public TopologyConstructor() {
//        this(Optional.empty());
//    }
//    public TopologyConstructor(Optional<AdminClient> adminClient) {
//        this.adminClient = adminClient;
//        if (this.adminClient.isPresent()) {
//            try {
//                topics.addAll(adminClient.get().listTopics().names().get());
//            } catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException("Error listing topics", e);
//            }
//        }
    }

    public void addDesiredTopic(String topicName, Optional<Integer> partitions) {
        // if requested with specific partition count, don't overwrite
        if (!desiredTopics.containsKey(topicName) || partitions.isPresent())
            desiredTopics.put(topicName, partitions);
    }

    public void ensureTopicExists(String topicName, Optional<Integer> partitionCount) {
        desiredTopics.put(topicName, partitionCount);
    }

    public void createTopicsAsNeeded(String kafkaHosts, String clientId) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafkaHosts);
        config.put("client.id", UUID.randomUUID().toString());

        AdminClient adminClient = AdminClient.create(config);
        Set<String> topics = new HashSet<String>();

        try {
            topics.addAll(adminClient.listTopics().names().get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error listing topics", e);
        }
        List<NewTopic> toBeCreated = desiredTopics.entrySet()
                .stream()
                .filter(e -> !topics.contains(e.getKey()))
                .map(e -> new NewTopic(e.getKey(), e.getValue(), Optional.empty()))
                .collect(Collectors.toList());
        try {
            adminClient.createTopics(toBeCreated).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Issue creating topics: " + desiredTopics.keySet(), e);
        }
    }
//
//    public void addConnectSink(String connectorResourceName, String topicName, Map<String, String> sinkParameters) {
//        System.err.println("addConnectSink>>>>>");
//
//        List<ConnectorTopicTuple> ctt = connectorAssociations.compute(connectorResourceName, (resourceName, list) -> list == null ? new ArrayList<>() : list);
//        ctt.add(new ConnectorTopicTuple(connectorResourceName, topicName, sinkParameters));
//    }

    public int generateNewPipeId() {
        return pipeCounter++;
    }

}
