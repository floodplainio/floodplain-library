package io.floodplain.reactive.source.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;

public class TopicSource implements TopologyPipeComponent {

    private final String topicName;

    private boolean materialize = false;

    public TopicSource(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        String source = ReplicationTopologyParser.addSourceStore(topology, topologyContext, topologyConstructor, Optional.empty(), topicName, Optional.empty(), this.materialize());
        topologyConstructor.addDesiredTopic(source, Optional.empty());
        transformerNames.push(source);
    }

    @Override
    public boolean materializeParent() {
        return false;
    }

    @Override
    public void setMaterialize() {
        this.materialize = true;
    }

    @Override
    public boolean materialize() {
        return this.materialize;
    }

}
