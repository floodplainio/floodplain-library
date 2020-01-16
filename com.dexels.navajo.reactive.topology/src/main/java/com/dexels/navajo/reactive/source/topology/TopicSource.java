package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.SourceMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;

import io.reactivex.Flowable;

public class TopicSource implements ReactiveSource,TopologyPipeComponent {

	private final SourceMetadata metadata;
	private final ReactiveParameters parameters;

	public TopicSource(SourceMetadata metadata, ReactiveParameters params) {
		this.metadata = metadata;
		this.parameters = params;
	}
	@Override
	public Flowable<DataItem> execute(StreamScriptContext context, Optional<ImmutableMessage> current,
			ImmutableMessage paramMessage) {
		return Flowable.error(new Exception("Topology sources shouldn't be executed"));
	}
	
	public int addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
		String name = resolved.paramString("name");
		String source = ReplicationTopologyParser.addSourceStore(topology, topologyContext, topologyConstructor, Optional.empty(), name, Optional.empty());
//		topology.addProcessor(filterName, filterProcessor, transformerNames.peek());
		System.err.println(">>> "+source);
		transformerNames.push(source);
		return pipeId;
	}
	
	@Override
	public boolean streamInput() {
		return false;
	}

	@Override
	public Type sourceType() {
		return Type.MESSAGE;
	}

	public ReactiveParameters parameters() {
		return parameters;
	}

	@Override
	public SourceMetadata metadata() {
		return metadata;
	}

}
