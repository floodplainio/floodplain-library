package io.floodplain.reactive.source.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.reactive.topology.ReactivePipe;
import io.floodplain.reactive.topology.ReactivePipeParser;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.base.Filters;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;

public class JoinWithTransformer implements TopologyPipeComponent {

    private final ReactivePipe joinWith;
    private final boolean isOptional;
    private final boolean multiple;
    private boolean materialize = false;

    public JoinWithTransformer(ReactivePipe joinWith) {
        this(false, false, joinWith);
    }

    // 'into' will be the
    public JoinWithTransformer(boolean isOptional, boolean multiple, ReactivePipe joinWith) {
        this.isOptional = isOptional;
        this.joinWith = joinWith;
        this.multiple = multiple;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        Optional<String> from = Optional.of(transformerNames.peek());
        Stack<String> pipeStack = new Stack<>();
        ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewPipeId(), pipeStack, joinWith, true);
        String with = pipeStack.peek();
        String name = topologyContext.qualifiedName("joinWith", transformerNames.size(), pipeId);
        Optional<String> filter = Optional.empty();
        Optional<Predicate<String, ReplicationMessage>> filterPredicate = Filters.getFilter(filter);

        ReplicationTopologyParser.addJoin(topology, topologyContext, topologyConstructor, from.get(), with, name, isOptional, multiple, filterPredicate, this.materialize);
        transformerNames.push(name);
    }

    //
    @Override
    public boolean materializeParent() {
        return true;
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
