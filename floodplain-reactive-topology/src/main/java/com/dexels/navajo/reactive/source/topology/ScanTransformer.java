package com.dexels.navajo.reactive.source.topology;

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
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;

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
	public void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
			TopologyContext topologyContext, TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		Optional<ContextExpression> keyExtractor = Optional.ofNullable(parameters.named.get("key"));
		ImmutableMessage initial = (ImmutableMessage) parameters.unnamed.get(0).apply().value;
		List<TopologyPipeComponent> onAdd = (List<TopologyPipeComponent>) parameters.unnamed.get(1).apply().value;
		List<TopologyPipeComponent> onRemove = (List<TopologyPipeComponent>) parameters.unnamed.get(2).apply().value;
		// TODO everything after the first is ignored
		Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyX = keyExtractor.map(e->(msg,state)->(String)e.apply(Optional.of(msg),Optional.of(state)).value);
		String reducerName = ReplicationTopologyParser.addReducer(topology, topologyContext, topologyConstructor, topologyContext.instance, transformerNames, currentPipeId, onAdd, onRemove, initial, materialize,keyX);
		transformerNames.push(reducerName);
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
