package io.floodplain.streams.processor.api;

import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

public interface TopologyTransformer {

    public void addTransformerToTopology(Topology topology, TopologyContext topologyContext, String instanceName,
                                         TopologyConstructor topologyConstructor, int pipeNumber, int transformerNumber);

}
