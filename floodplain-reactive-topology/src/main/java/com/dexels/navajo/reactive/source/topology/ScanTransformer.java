package com.dexels.navajo.reactive.source.topology;

import java.util.List;
import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class ScanTransformer implements ReactiveTransformer,TopologyPipeComponent {

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialize;

	public ScanTransformer(TransformerMetadata metadata, ReactiveParameters params) {
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
	@SuppressWarnings("unchecked")
	@Override
	public void addToTopology(String namespace, Stack<String> transformerNames, int currentPipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
//		junit_junit_1_debezium_0_debezium-Generic-test-dvd.public.payment
//		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
		Optional<ContextExpression> keyExtractor = Optional.ofNullable(parameters.named.get("key"));
		ImmutableMessage initial = (ImmutableMessage) parameters.unnamed.get(0).apply().value;
//		ImmutableMessage initial = (ImmutableMessage) resolved.unnamedParameters().get(0).value;
//		Optional<String> key = resolved.optionalString("key");
		List<TopologyPipeComponent> onAdd = (List<TopologyPipeComponent>) parameters.unnamed.get(1).apply().value;
		List<TopologyPipeComponent> onRemove = (List<TopologyPipeComponent>) parameters.unnamed.get(2).apply().value;
//		String parentProcessor = transformerNames.peek();
//		Stack<String> removeProcessorStack = new Stack<>();
//		removeProcessorStack.addAll(transformerNames);
//		onAdd.stream().map(e->e.)
//		ReplicationTopologyParser.addReducer(initial,onAdd,onRemove);
		// TODO everything after the first is ignored
//		onAdd.get(0).addToTopology(namespace, transformerNames, currentPipeId, topology, topologyContext, topologyConstructor, initial);
		String reducerName = ReplicationTopologyParser.addReducer(topology, topologyContext, topologyConstructor, namespace, transformerNames, currentPipeId, onAdd, onRemove, initial, materialize,keyExtractor);
		transformerNames.push(reducerName);
		//		addReducer(topology, topologyContext, topologyConstructor, namespace, transformerNames, currentPipeId, onAdd, onRemove, stateMessage);
	}
//	
//	private static void addReducer(final Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor,
//			String namespace, Stack<String> transformerNames, int currentPipeId,List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove, ImmutableMessage stateMessage) {
//
//		String parentName = transformerNames.peek();
//		String ifElseName = namespace+"_"+currentPipeId+"_"+"ifelse"+transformerNames.size();
//		transformerNames.push(ifElseName);
//		String trueBranchName = namespace+"_"+currentPipeId+"_"+"addbranch"+transformerNames.size();
//		String falseBranchName = namespace+"_"+currentPipeId+"_"+"removebranch"+transformerNames.size();
//
//		String reduceName = namespace+"_"+currentPipeId+"_"+"reduce"+transformerNames.size();
//		String reduceStoreName = "STORE_"+reduceName;
//		Stack<String> addProcessorStack = new Stack<>();
//		addProcessorStack.addAll(transformerNames);
//
//		Stack<String> removeProcessorStack = new Stack<>();
//		removeProcessorStack.addAll(transformerNames);
//
//		topology.addProcessor(ifElseName, ()->new IfElseProcessor(msg->false, trueBranchName, Optional.of(falseBranchName)), parentName);
//		for (TopologyPipeComponent addBranchComponents : onAdd) {
//			addBranchComponents.addToTopology(namespace, addProcessorStack, currentPipeId, topology, topologyContext, topologyConstructor, stateMessage);
//		}
//		for (TopologyPipeComponent removePipeComponents : onRemove) {
//			removePipeComponents.addToTopology(namespace, removeProcessorStack, currentPipeId, topology, topologyContext, topologyConstructor, stateMessage);
//		}
//		// TODO I think there is something wrong in the bookkeeping of the transformerStack
//		topology.addProcessor(reduceName, ()->new StoreStateProcessor(reduceName, reduceStoreName, stateMessage), addProcessorStack.peek(),removeProcessorStack.peek());
//
//		
//	}

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
