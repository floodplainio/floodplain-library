package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
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

public class JoinWithTransformer implements TopologyPipeComponent{

	private final ReactivePipe joinWith;
	private final Optional<String> into;
	private boolean materialize = false;

	// 'into' will be the
	public JoinWithTransformer(ReactivePipe joinWith, Optional<String> into) {
		this.joinWith = joinWith;
		this.into = into;
	}

	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
		TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		Optional<String> from = Optional.of(transformerNames.peek());
		Stack<String> pipeStack = new Stack<>();
		ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewPipeId(),pipeStack, joinWith,true);
//		Optional<String> into = resolved.optionalString("into");
//		boolean isList = into.isPresent();
		String with = pipeStack.peek();
//		String name = createName(topologyContext, transformerNames.size(), pipeId);
		String name = topologyContext.qualifiedName("joinWith",transformerNames.size(), pipeId);
		Optional<String> filter = Optional.empty();
		boolean isOptional = false;

        //        ReplicationTopologyParser.createJoinFunction(isList, into, name, columns, keyField, valueField);
        final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction = (msg,comsg)->msg.withParamMessage(comsg.message());

        Optional<Predicate<String, ReplicationMessage>> filterPredicate = Filters.getFilter(filter);

		ReplicationTopologyParser.addJoin(topology, topologyContext, topologyConstructor, from.get(), with, name, isOptional, into, filterPredicate,this.materialize);
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
