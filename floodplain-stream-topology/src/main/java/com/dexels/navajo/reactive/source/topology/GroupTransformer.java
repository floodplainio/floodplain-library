package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GroupTransformer implements TopologyPipeComponent {

    private final BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor;
    private boolean materialize;

    public GroupTransformer(BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        Function<ReplicationMessage, String> keyExtractor = msg -> {
            return this.keyExtractor.apply(msg.message(), msg.paramMessage().orElse(ImmutableFactory.empty()));
        };

        addGroupTransformer(transformerNames, pipeId, topology, topologyContext, topologyConstructor, keyExtractor, "group");

    }

    public static void addGroupTransformer(Stack<String> transformerNames, int pipeId, Topology topology,
                                           TopologyContext topologyContext, TopologyConstructor topologyConstructor, Function<ReplicationMessage, String> keyExtractor, String transformerName) {
        String from = transformerNames.peek();
//		String name = createName(topologyContext, transformerName,transformerNames.size(),pipeId);
        String name = topologyContext.qualifiedName(transformerName, transformerNames.size(), pipeId);
        boolean ignoreOriginalKey = false;
        String grouped = ReplicationTopologyParser.addGroupedProcessor(topology, topologyContext, topologyConstructor, name, from, ignoreOriginalKey, keyExtractor, Optional.empty());
        transformerNames.push(grouped);
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
