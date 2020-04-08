package com.dexels.kafka.streams.remotejoin;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopologyConstructor {

    public class ConnectorTopicTuple {
        public final String connectorResourceName;
        public final Map<String, String> sinkParameters;
        public final String topicName;

        public ConnectorTopicTuple(String connectorResourceName, String topicName, Map<String, String> sinkParameters) {
            this.connectorResourceName = connectorResourceName;
            this.sinkParameters = sinkParameters;
            this.topicName = topicName;
        }
    }

    public final Map<String, List<ConnectorTopicTuple>> connectorAssociations = new HashMap<>();
    public final Map<String, List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ImmutableMessage>>> immutableStoreSupplier = new HashMap<>();
    // TODO: Could be optional, only needed in xml based stream code
    public final Optional<AdminClient> adminClient;
    public final Set<String> stores = new HashSet<>();
    public final Map<String, String> sources = new HashMap<>();
    // TODO could race conditions happen? If so, would that be a problem?
    public final Set<String> topics = new HashSet<String>();

    private int pipeCounter = 1;

    public TopologyConstructor() {
        this(Optional.empty());
    }
    public TopologyConstructor(Optional<AdminClient> adminClient) {
        this.adminClient = adminClient;
        if (this.adminClient.isPresent()) {
            try {
                topics.addAll(adminClient.get().listTopics().names().get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Error listing topics", e);
            }
        }
    }

    public void ensureTopicExists(String topicName, Optional<Integer> partitionCount) {
        if (topics.contains(topicName)) {
            return;
        }
        KafkaUtils.ensureExistsSync(adminClient, topicName, partitionCount);
    }

    public void addConnectSink(String connectorResourceName, String topicName, Map<String, String> sinkParameters) {
        List<ConnectorTopicTuple> ctt = connectorAssociations.compute(connectorResourceName, (resourceName, list) -> list == null ? new ArrayList<>() : list);
        ctt.add(new ConnectorTopicTuple(connectorResourceName, topicName, sinkParameters));
    }

    public int generateNewPipeId() {
        return pipeCounter++;
    }

}
