package com.dexels.navajo.reactive.source.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;

import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class SetTransformer implements ReactiveTransformer, TopologyPipeComponent {

	private boolean materialize;
	private final SetFactory metadata;
	private final ReactiveParameters parameters;
	Function<StreamScriptContext, Function<ReplicationMessage, ReplicationMessage>> transformer;
	
	
	private final static Logger logger = LoggerFactory.getLogger(SetTransformer.class);

	public SetTransformer(SetFactory metadata, ReactiveParameters parameters) {
		this.metadata = metadata;
		this.parameters = parameters;
		this.transformer = execute(parameters);
	}
	
	// note copied from SetSingle
	public Function<StreamScriptContext,Function<ReplicationMessage,ReplicationMessage>> execute(ReactiveParameters params) {
		return context->(item)->{
			ImmutableMessage s = item.message();
			ReactiveResolvedParameters parms = params.resolve(context, Optional.of(s), item.paramMessage().orElse(ImmutableFactory.empty()), metadata);
			boolean condition = parms.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}
			// will use the second message as input, if not present, will use the source message
			for (Entry<String,Operand> elt : parms.namedParameters().entrySet()) {
				if(!elt.getKey().equals("condition")) {
					s = addColumn(s, elt.getKey(), elt.getValue());
				}
			}
			return ReplicationFactory.standardMessage(s).withOperation(item.operation()).withPrimaryKeys(item.primaryKeys());
		};
	
	}
	
	// note copied from SetSingle
	private ImmutableMessage addColumn(ImmutableMessage input, String name, Operand value) {
		return addColumn(input, Arrays.asList(name.split("/")), value);
	}

	// note copied from SetSingle
	private ImmutableMessage addColumn(ImmutableMessage input, List<String> path, Operand value) {
//		logger.info("Setting path: {} value: {} type: {}",path,value.value,value.type);
		if(path.size()>1) {
			String submessage = path.get(0);
			Optional<ImmutableMessage> im = input.subMessage(submessage);
			List<String> popped = new ArrayList<>(path);
			popped.remove(0);
			if(im.isPresent()) {
				return input.withSubMessage(submessage, addColumn(im.get(),popped,value));
			} else {
				ImmutableMessage nw = addColumn(ImmutableFactory.empty(), popped, value);
				return input.withSubMessage(submessage, nw);
			}
		} else {
			// TODO use enum
			if("immutable".equals(value.type)) {
				ImmutableMessage im = value.immutableMessageValue();
				return input.withSubMessage(path.get(0), im);
			} else if("immutableList".equals(value.type)) {
				List<ImmutableMessage> immutableMessageList= value.immutableMessageList();
				return input.withSubMessages(path.get(0), immutableMessageList);
			} else {
				return input.with(path.get(0), value.value, value.type);
			}
		}
		
	}

	
	@Override
	public void addToTopology(String namespace, Stack<String> transformerNames, int currentPipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		StreamScriptContext ssc = StreamScriptContext.fromTopologyContext(topologyContext);
		Function<ReplicationMessage, ReplicationMessage> apply = transformer.apply(ssc);
		FunctionProcessor fp = new FunctionProcessor(apply);
		String name = createName(namespace, this.metadata.name(),transformerNames.size(), currentPipeId);
		logger.info("Adding processor: {} to parent: {} hash: {}",name,transformerNames,transformerNames.hashCode());

		
		if (this.materialize()) {
			topology.addProcessor(name+"_prematerialize", ()->fp, transformerNames.peek());
			ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name+"_prematerialize");
		} else {
			topology.addProcessor(name, ()->fp, transformerNames.peek());

		}
		transformerNames.push(name);
	}
	
	private  String createName(String namespace, String name, int transformerNumber, int pipeId) {
		return namespace+"_"+pipeId+"_"+name+"_"+transformerNumber;
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
