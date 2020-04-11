package com.dexels.navajo.reactive.source.topology;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.Filters;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.navajo.reactive.topology.ReactivePipe;
import com.dexels.navajo.reactive.topology.ReactivePipeParser;
import com.dexels.replication.api.ReplicationMessage;
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
        this(false,false,joinWith);
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
        final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction = (msg, comsg) -> msg.withParamMessage(comsg.message());

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
