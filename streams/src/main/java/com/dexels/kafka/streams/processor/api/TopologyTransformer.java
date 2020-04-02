package com.dexels.kafka.streams.processor.api;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

public interface TopologyTransformer {

    public void addTransformerToTopology(Topology topology, TopologyContext topologyContext, String instanceName,
                                         TopologyConstructor topologyConstructor, int pipeNumber, int transformerNumber);

}
