package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.Topology;

import java.util.Stack;
import java.util.function.Function;

public class RowNumberTransformer implements TopologyPipeComponent {

	private boolean materialize;

	public RowNumberTransformer() {
	}

	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
//		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
//		ContextExpression keyExtract  = parameters.named.get("key");
//		Function<ReplicationMessage,String> keyExtractor = msg->{
//			return keyExtract.apply(Optional.of(msg.message()), msg.paramMessage()).stringValue();
//		};
//		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
// TODO why is here a key extractor / group transformer? Copy paste error?
//		addGroupTransformer(transformerNames, pipeId, topology, topologyContext, topologyConstructor, keyExtractor,"rownum");
		String from = transformerNames.peek();
		String name = topologyContext.qualifiedName("rownum",transformerNames.size(), pipeId);
		String rowNum = ReplicationTopologyParser.addKeyRowProcessor(topology, topologyContext, topologyConstructor, from, name,this.materialize);
		transformerNames.push(rowNum);
	}
	public static void addGroupTransformer(Stack<String> transformerNames, int pipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor, Function<ReplicationMessage,String> keyExtractor, String transformerName) {

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
		return this.materialize;
	}




}
