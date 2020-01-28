package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser.Flatten;
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

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class JoinRemoteTransformer implements ReactiveTransformer ,TopologyPipeComponent{

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialize = false;

	public JoinRemoteTransformer(TransformerMetadata metadata, ReactiveParameters params) {
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
	public int addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
		String key = resolved.paramString("key");
		GroupTransformer.addGroupTransformer(transformerNames,pipeId,topology,topologyContext,topologyConstructor,key,metadata.name());
		
		Optional<String> from = Optional.of(transformerNames.peek());

		Operand o = resolved.unnamedParameters().stream().findFirst().orElseThrow(()->new TopologyDefinitionException("Missing parameters for joinWith, should have one sub stream"));
		ReactivePipe rp = (ReactivePipe)o.value;
		Stack<String> pipeStack = new Stack<>();
		pipeId = ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, pipeId,pipeStack, rp);
		boolean isList = false;
		String with = pipeStack.peek();
		
		String name = pipeId+"_"+metadata.name()+"_"+transformerNames.size();
		Optional<String> filter = Optional.empty();
		boolean isOptional = true;
//        BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> listJoinFunction = ReplicationTopologyParser.createListJoinFunction(into, from.get(), Optional.empty());
//        BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> listJoinFunction = ReplicationTopologyParser.createParamListJoinFunction(into);

        
        //        ReplicationTopologyParser.createJoinFunction(isList, into, name, columns, keyField, valueField);
//        final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction = CoreOperators.getJoinFunction(Optional.of(into),Optional.<String>empty());

//        Optional<Predicate<String, ReplicationMessage>> filterPredicate = Filters.getFilter(filter);

        Optional<String> into = Optional.of("monkeymonkey");
        boolean optional = false;
        
        ReplicationTopologyParser.addSingleJoinGrouped(topology, topologyContext, topologyConstructor, from.get(), into, name, Optional.empty(), Optional.empty(), Flatten.NONE, isList, with, optional);
//		ReplicationTopologyParser.addJoin(topology, topologyContext, topologyConstructor, from.get(), isList, with, name, isOptional, listJoinFunction, joinFunction, filterPredicate);
		System.err.println("Pushinh: "+name);
		transformerNames.push(name);
		return pipeId;
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
