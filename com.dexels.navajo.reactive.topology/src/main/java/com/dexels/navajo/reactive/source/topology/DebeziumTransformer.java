package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.debezium.impl.DebeziumConversionProcessor;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class DebeziumTransformer implements ReactiveTransformer,TopologyPipeComponent {

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialized = false;

	public DebeziumTransformer(TransformerMetadata metadata, ReactiveParameters params) {
		this.metadata = metadata;
		this.parameters = params;
	}
	@Override
	public FlowableTransformer<DataItem, DataItem> execute(StreamScriptContext context,
			Optional<ImmutableMessage> current, ImmutableMessage param) {
		return item->Flowable.error(()->new ReactiveParseException("Group transformer shouldn't be executed"));
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
		String topic = resolved.paramString("topic");
		boolean appendTenant = resolved.optionalBoolean("appendTenant").orElse(false);
		boolean appendSchema = resolved.optionalBoolean("appendSchema").orElse(false);
		DebeziumConversionProcessor processor = new DebeziumConversionProcessor(topic, topologyContext, appendTenant, appendSchema,false);
		String from = transformerNames.peek();
		String name = createName(transformerNames.size(),pipeId);
		if (this.materialized) {
			topology.addProcessor(name+"_prematerialize", ()->processor, from);
			ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name+"_prematerialize");
		} else {
			topology.addProcessor(name, ()->processor, from);

		}
		//		String sourc2 = ReplicationTopologyParser.addSourceStore(topology, topologyContext, topologyConstructor, Optional.empty(), name, Optional.empty());
//		topology.addProcessor(filterName, filterProcessor, transformerNames.peek());
		System.err.println(">>> "+name);
		transformerNames.push(name);
		return pipeId;

	}
	
	// TODO address multiple pipes
	private  String createName(int transformerNumber, int pipeId) {
		return pipeId+"_"+metadata.name()+"_"+transformerNumber;
	}
	@Override
	public boolean materializeParent() {
		return false;
	}
	@Override
	public void setMaterialize() {
		this.materialized = true;
	}

	@Override
	public boolean materialize() {
		return this.materialized;
	}
}