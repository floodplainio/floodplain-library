package com.dexels.navajo.reactive.source.topology;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;

import org.apache.kafka.streams.Topology;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveMerger;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.mappers.SetSingle;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

public class Set implements TopologyPipeComponent, ReactiveMerger {

	
	private SetSingle single;
	private Function<StreamScriptContext, Function<DataItem, DataItem>> instance;

	public Set() {
		single = new SetSingle();

	}
	@Override
	public Function<StreamScriptContext, Function<DataItem, DataItem>> execute(ReactiveParameters params) {
		instance = single.execute(params);
		return instance;
	}

	@Override
	public int addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
		StreamScriptContext ssc = StreamScriptContext.fromTopologyContext(topologyContext);
		FunctionProcessor fp = new FunctionProcessor(instance.apply(ssc));
		String name = this.getClass().getName().toLowerCase();
		topology.addProcessor(name, ()->fp, transformerNames.peek());
		transformerNames.push(createName(name, transformerNames.size(), currentPipeId));
		return currentPipeId;
	}

	private  String createName(String name, int transformerNumber, int pipeId) {
		return pipeId+"_"+name+"_"+transformerNumber;
	}
	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.empty();
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.empty();
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		return Optional.empty();
	}

}
