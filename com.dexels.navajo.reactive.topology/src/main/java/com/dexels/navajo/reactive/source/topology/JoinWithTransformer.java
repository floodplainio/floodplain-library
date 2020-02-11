package com.dexels.navajo.reactive.source.topology;

import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Predicate;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.Filters;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.kafka.streams.remotejoin.TopologyDefinitionException;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactivePipe;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.navajo.reactive.topology.ReactivePipeParser;
import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class JoinWithTransformer implements ReactiveTransformer ,TopologyPipeComponent{

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialize = false;

	public JoinWithTransformer(TransformerMetadata metadata, ReactiveParameters params) {
		this.metadata = metadata;
		this.parameters = params;
	}
	@Override
	public FlowableTransformer<DataItem, DataItem> execute(StreamScriptContext context,
			Optional<ImmutableMessage> current, ImmutableMessage param) {
		return item->Flowable.error(()->new ReactiveParseException("Topology transformer shouldn't be executed"));
	}

	@Override
	public TransformerMetadata metadata() {
		return metadata;
	}
	
	@Override
	public ReactiveParameters parameters() {
		return parameters;
	}
	@Override
	public void addToTopology(String namespace, Stack<String> transformerNames, int pipeId, Topology topology,
		TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
		Optional<String> from = Optional.of(transformerNames.peek());
		Operand o = resolved.unnamedParameters().stream().findFirst().orElseThrow(()->new TopologyDefinitionException("Missing parameters for joinWith, should have one sub stream"));
		ReactivePipe rp = (ReactivePipe)o.value;
		Stack<String> pipeStack = new Stack<>();
		ReactivePipeParser.processPipe(namespace, topologyContext, topologyConstructor, topology, topologyConstructor.generateNewPipeId(),pipeStack, rp,true);
		boolean isList = true;
		String with = pipeStack.peek();
		String into = resolved.paramString("into");
		String name = createName(namespace, transformerNames.size(), pipeId);
		Optional<String> filter = Optional.empty();
		boolean isOptional = false;
        BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> listJoinFunction = ReplicationTopologyParser.createParamListJoinFunction(into);

        //        ReplicationTopologyParser.createJoinFunction(isList, into, name, columns, keyField, valueField);
        final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction = CoreOperators.getJoinFunction(Optional.of(into),Optional.<String>empty());

        Optional<Predicate<String, ReplicationMessage>> filterPredicate = Filters.getFilter(filter);

		ReplicationTopologyParser.addJoin(topology, topologyContext, topologyConstructor, from.get(), isList, with, name, isOptional, listJoinFunction, joinFunction, filterPredicate,this.materialize);
		transformerNames.push(name);
	}

	private  String createName(String namespace, int transformerNumber, int pipeId) {
		return namespace+"_"+pipeId+"_"+metadata.name()+"_"+transformerNumber;
//		return pipeId+"_"+metadata.name()+"_"+transformerNumber;
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
