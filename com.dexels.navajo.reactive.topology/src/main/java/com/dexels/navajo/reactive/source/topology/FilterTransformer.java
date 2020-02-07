package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveParseException;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.TransformerMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class FilterTransformer implements ReactiveTransformer,TopologyPipeComponent {

	private final ReactiveParameters parameters;
	private final TransformerMetadata metadata;
	private final ProcessorSupplier<String,ReplicationMessage> filterProcessor;
	private boolean materialized = false;
	
	private final static Logger logger = LoggerFactory.getLogger(FilterTransformer.class);


	public FilterTransformer(TransformerMetadata metadata, ReactiveParameters parameters) {
		this.parameters = parameters;
		this.metadata = metadata;
		this.filterProcessor = ()->new FilterProcessor(parameters.unnamed.get(0));
	}

	@Override
	public FlowableTransformer<DataItem, DataItem> execute(StreamScriptContext context,Optional<ImmutableMessage> current, ImmutableMessage param) {
		return item->Flowable.error(()->new ReactiveParseException("Group transformer is a topology and shouldn't be executed"));

	}

	public int addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor) {
		String filterName = createName(transformerNames.size(), pipeId);
		logger.info("Stack top for transformer: {}", transformerNames.peek());
		if (this.materialized) {
			topology.addProcessor(filterName+"_prematerialize",filterProcessor, transformerNames.peek());
			ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, filterName, filterName+"_prematerialize");
		} else {
			topology.addProcessor(filterName, filterProcessor, transformerNames.peek());

		}
		transformerNames.push(filterName);
		return pipeId;
	}
	
//	private static String createParentName(String sourceName, int transformerNumber) {
//		
//		if(transformerNumber==0) {
//			return sourceName;
//		}
//		return sourceName+"_"+transformerNumber;
//	}
//	
	private  String createName(int transformerNumber, int pipeId) {
		return pipeId+"_"+metadata.name()+"_"+transformerNumber;
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
		this.materialized  = true;
	}

	@Override
	public boolean materialize() {
		return this.materialized;
	}

}
