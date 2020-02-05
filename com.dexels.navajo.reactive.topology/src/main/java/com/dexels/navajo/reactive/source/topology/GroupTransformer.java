package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class GroupTransformer implements ReactiveTransformer,TopologyPipeComponent {

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialize;

	public GroupTransformer(TransformerMetadata metadata, ReactiveParameters params) {
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
//		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ContextExpression keyExtract  = parameters.named.get("key");
		Function<ReplicationMessage,String> keyExtractor = msg->{
			String result = keyExtract.apply(null, Optional.of(msg.message()), msg.paramMessage()).stringValue();
			System.err.println("Extracting key from message: "+result);
			return result;
		};
//		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
		addGroupTransformer(transformerNames, pipeId, topology, topologyContext, topologyConstructor, keyExtractor,metadata.name());
		return pipeId;

	}
	public static void addGroupTransformer(Stack<String> transformerNames, int pipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor, Function<ReplicationMessage,String> keyExtractor, String transformerName) {
		String from = transformerNames.peek();
		String name = createName(transformerName,transformerNames.size(),pipeId);
		boolean ignoreOriginalKey = false;
		String grouped = ReplicationTopologyParser.addGroupedProcessor(topology, topologyContext, topologyConstructor, name, from, ignoreOriginalKey, keyExtractor, Optional.empty());
		transformerNames.push(grouped);
	}
	
	private static String createName(String name, int transformerNumber, int pipeId) {
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
		return this.materialize;
	}




}
