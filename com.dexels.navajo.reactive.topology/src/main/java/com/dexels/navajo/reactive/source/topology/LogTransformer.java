package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
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

public class LogTransformer implements ReactiveTransformer, TopologyPipeComponent {

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialize = false;

	
	private final static Logger logger = LoggerFactory.getLogger(LogTransformer.class);

	public static final String SINK_PREFIX = "SINK_";
	public static final String SINKLOG_PREFIX = "SINK_LOG_";
	public LogTransformer(TransformerMetadata metadata, ReactiveParameters params) {
		this.metadata = metadata;
		this.parameters = params;
	}
	@Override
	public FlowableTransformer<DataItem, DataItem> execute(StreamScriptContext context,
			Optional<ImmutableMessage> current, ImmutableMessage param) {
		return item->Flowable.error(()->new ReactiveParseException("Sink transformer shouldn't be executed"));
	}

	@Override
	public void addToTopology(String namespace, Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
//		String filterName = createName(transformerNames.size(), pipeId);
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
		Optional<Integer> every = resolved.optionalInteger("every");
		boolean dumpStack = resolved.optionalBoolean("dumpStack").orElse(false);
		if(every.isPresent()) {
			throw new UnsupportedOperationException("'every' param not yet implemented in LogTransformer");
		}
		String logName = resolved.paramString("logName");
		logger.info("Stack top for transformer: "+transformerNames.peek());
		String name = createName(namespace, transformerNames.size(), pipeId);
		if (this.materialize()) {
//			topology.addProcessor(filterName+"_prematerialize",filterProcessor, transformerNames.peek());
			topology.addProcessor(name+"_prematerialize", ()->new LogProcessor(logName,dumpStack), transformerNames.peek());
			ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name+"_prematerialize");
		} else {
			topology.addProcessor(name, ()->new LogProcessor(logName,dumpStack), transformerNames.peek());
		}
		transformerNames.push(name);
	}
	
	private  String createName(String namespace, int transformerNumber, int pipeId) {
		return namespace+"_"+pipeId+"_"+metadata.name()+"_"+transformerNumber;
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
	public boolean materializeParent() {
		return false;
	}
	@Override
	public void setMaterialize() {
		this.materialize  = true;
	}

	@Override
	public boolean materialize() {
		return this.materialize;
	}

}
