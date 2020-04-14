package io.floodplain.reactive.source.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Stack;
import java.util.function.Function;

public class RowNumberTransformer implements TopologyPipeComponent {

    private boolean materialize;

    public RowNumberTransformer() {
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        String from = transformerNames.peek();
        String name = topologyContext.qualifiedName("rownum", transformerNames.size(), pipeId);
        String rowNum = ReplicationTopologyParser.addKeyRowProcessor(topology, topologyContext, topologyConstructor, from, name, this.materialize);
        transformerNames.push(rowNum);
    }

    public static void addGroupTransformer(Stack<String> transformerNames, int pipeId, Topology topology,
                                           TopologyContext topologyContext, TopologyConstructor topologyConstructor, Function<ReplicationMessage, String> keyExtractor, String transformerName) {

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
