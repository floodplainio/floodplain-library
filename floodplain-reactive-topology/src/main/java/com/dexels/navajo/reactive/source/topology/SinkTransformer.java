package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.kafka.streams.serializer.ConnectReplicationMessageSerde;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.*;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.dexels.kafka.streams.api.CoreOperators.topicName;

public class SinkTransformer implements ReactiveTransformer, TopologyPipeComponent {

	private TransformerMetadata metadata;
	private ReactiveParameters parameters;
	private boolean materialize = false;

	
	private final static Logger logger = LoggerFactory.getLogger(SinkTransformer.class);

	public static final String SINK_PREFIX = "SINK_";
	public static final String SINKLOG_PREFIX = "SINK_LOG_";
	public SinkTransformer(TransformerMetadata metadata, ReactiveParameters params) {
		this.metadata = metadata;
		this.parameters = params;
	}
	@Override
	public FlowableTransformer<DataItem, DataItem> execute(StreamScriptContext context,
			Optional<ImmutableMessage> current, ImmutableMessage param) {
		return item->Flowable.error(()->new ReactiveParseException("Sink transformer shouldn't be executed"));
	}

	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);
		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);
	
		boolean create = resolved.optionalBoolean("create").orElse(false);
		Optional<Integer> partitions = resolved.optionalInteger("partitions");
		List<Operand> operands = resolved.unnamedParameters();
		Optional<String> sinkName = resolved.optionalString("connector");
		Optional<Boolean> connect = resolved.optionalBoolean("connect");
		for (Operand operand : operands) {
	        String sinkTopic = topicName( operand.stringValue(), topologyContext);
	        // TODO shouldn't we use the createName?
	        // TODO still weird if we use multiple
			if(create) {
				topologyConstructor.ensureTopicExists(sinkTopic,partitions);
			}
			logger.info("Stack top for transformer: "+transformerNames.peek());
			Map<String,String> values = resolved.namedParameters().entrySet().stream().filter(e->!e.getKey().equals("connect")) .collect(Collectors.toMap(e->e.getKey(), e->(String)e.getValue().value));
			Map<String,String> withTopic = new HashMap<>(values);
			withTopic.put("topic", sinkTopic);
			sinkName.ifPresent(sink->topologyConstructor.addConnectSink(sink,sinkTopic, values));
			if(connect.isPresent() && connect.get()) {
				ConnectReplicationMessageSerde crms = new ConnectReplicationMessageSerde();
				topology.addSink(SINK_PREFIX+sinkTopic,sinkTopic,Serdes.String().serializer(),crms.serializer(), transformerNames.peek());
			} else {
				topology.addSink(SINK_PREFIX+sinkTopic, sinkTopic, transformerNames.peek());
			}
		}
	}

//	private  String createName(String namespace, int transformerNumber, int pipeId) {
//		return namespace+"_"+pipeId+"_"+metadata.name()+"_"+transformerNumber;
//	}
	
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
