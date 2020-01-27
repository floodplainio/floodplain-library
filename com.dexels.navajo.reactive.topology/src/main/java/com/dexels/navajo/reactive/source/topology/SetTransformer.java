package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.mappers.SetSingle;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class SetTransformer implements ReactiveTransformer, TopologyPipeComponent {

	private boolean materialize;
	private final SetFactory metadata;
	private final ReactiveParameters parameters;
	SetSingle single = new SetSingle();
	Function<StreamScriptContext,Function<DataItem,DataItem>> transformer;
	
	public SetTransformer(SetFactory metadata, ReactiveParameters parameters) {
		this.metadata = metadata;
		this.parameters = parameters;
		this.transformer = this.single.execute(parameters);
	}
	
	@Override
	public int addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		StreamScriptContext ssc = StreamScriptContext.fromTopologyContext(topologyContext);
		FunctionProcessor fp = new FunctionProcessor(transformer.apply(ssc));
		String name = createName(this.metadata.name(),transformerNames.size(), currentPipeId);
		topology.addProcessor(name, ()->fp, transformerNames.peek());
//		System.err.println("bloop: "+);
		transformerNames.push(name);
		return currentPipeId;
	}
	
	private  String createName(String name, int transformerNumber, int pipeId) {
		return pipeId+"_"+name+"_"+transformerNumber;
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

	@Override
	public FlowableTransformer<DataItem, DataItem> execute(StreamScriptContext context,
			Optional<ImmutableMessage> current, ImmutableMessage param) {
		return item->Flowable.error(()->new ReactiveParseException("Sink transformer shouldn't be executed"));
	}

	@Override
	public TransformerMetadata metadata() {
		return this.metadata;
	}

	@Override
	public ReactiveParameters parameters() {
		return this.parameters;
	}

}
