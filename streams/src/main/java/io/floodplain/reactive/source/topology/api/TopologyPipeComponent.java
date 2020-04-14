package io.floodplain.reactive.source.topology.api;

import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Stack;

public interface TopologyPipeComponent {
    public void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor);

    public boolean materializeParent();

    public void setMaterialize();

    public boolean materialize();

}
