package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;
import java.util.function.BiFunction;

public class SetTransformer implements TopologyPipeComponent {

    private boolean materialize;
    //	private final boolean fromEmpty;
    private final BiFunction<ImmutableMessage, ImmutableMessage, ImmutableMessage> transformer;


    private final static Logger logger = LoggerFactory.getLogger(SetTransformer.class);

    public SetTransformer(BiFunction<ImmutableMessage, ImmutableMessage, ImmutableMessage> transformer) {
        this.transformer = transformer;
    }


    @Override
    public void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        FunctionProcessor fp = new FunctionProcessor(this.transformer);
        String name = topologyContext.qualifiedName("set", transformerNames.size(), currentPipeId);
//		String name = createName(topologyContext, this.metadata.name(),transformerNames.size(), currentPipeId);
        logger.info("Adding processor: {} to parent: {} hash: {}", name, transformerNames, transformerNames.hashCode());


        if (this.materialize()) {
            topology.addProcessor(name + "_prematerialize", () -> fp, transformerNames.peek());
            ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name + "_prematerialize");
        } else {
            topology.addProcessor(name, () -> fp, transformerNames.peek());

        }
        transformerNames.push(name);
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
        return materialize;
    }


}