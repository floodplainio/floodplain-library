package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser.Flatten;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.navajo.reactive.topology.ReactivePipe;
import com.dexels.navajo.reactive.topology.ReactivePipeParser;
import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;

public class JoinRemoteTransformer implements TopologyPipeComponent {

    private final ReactivePipe remoteJoin;
    private boolean materialize = false;
    private final BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor;

    public JoinRemoteTransformer(ReactivePipe remoteJoin, BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor) {
        this.remoteJoin = remoteJoin;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {

        Function<ReplicationMessage, String> keyXtr = msg -> {
            return this.keyExtractor.apply(msg.message(), msg.paramMessage().orElse(ImmutableFactory.empty()));
        };
        GroupTransformer.addGroupTransformer(transformerNames, pipeId, topology, topologyContext, topologyConstructor, keyXtr, "joinRemote");

        Optional<String> from = Optional.of(transformerNames.peek());
        Stack<String> pipeStack = new Stack<>();
        ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewPipeId(), pipeStack, remoteJoin, true);
        boolean isList = false;
        String with = pipeStack.peek();

        String name = topologyContext.instance + "_" + pipeId + "_" + "joinRemote" + "_" + transformerNames.size();
        Optional<String> into = Optional.of("monkeymonkey");
        boolean optional = false;

        ReplicationTopologyParser.addSingleJoinGrouped(topology, topologyContext, topologyConstructor, from.get(), into, name, Optional.empty(), Optional.empty(), Flatten.NONE, isList, with, optional);
        transformerNames.push(name);
    }

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
