package com.dexels.navajo.reactive.source.topology;

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
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.reactive.api.*;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.navajo.reactive.topology.ReactivePipeParser;
import com.dexels.replication.api.ReplicationMessage;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;

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
	public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ContextExpression keyExtract  = parameters.named.get("key");
		Function<ReplicationMessage,String> keyExtractor = msg->keyExtract.apply(Optional.of(msg.message()), msg.paramMessage()).stringValue();
		
		GroupTransformer.addGroupTransformer(transformerNames,pipeId,topology,topologyContext,topologyConstructor,keyExtractor,metadata.name());
		
		Optional<String> from = Optional.of(transformerNames.peek());
		ReactiveResolvedParameters resolved = parameters.resolveUnnamed(context,Optional.empty(), ImmutableFactory.empty(), metadata);
		
		Operand o = resolved.unnamedParameters().stream().findFirst().orElseThrow(()->new TopologyDefinitionException("Missing parameters for joinWith, should have one sub stream"));
		ReactivePipe rp = (ReactivePipe)o.value;
		Stack<String> pipeStack = new Stack<>();
		ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewPipeId(),pipeStack, rp,true);
		boolean isList = false;
		String with = pipeStack.peek();
		
		String name = topologyContext.instance+"_"+pipeId+"_"+metadata.name()+"_"+transformerNames.size();
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
