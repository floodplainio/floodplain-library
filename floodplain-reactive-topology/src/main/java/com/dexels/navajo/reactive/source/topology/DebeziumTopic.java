package com.dexels.navajo.reactive.source.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.debezium.impl.DebeziumConversionProcessor;
import com.dexels.kafka.streams.remotejoin.IdentityProcessor;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.StoreProcessor;
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

public class DebeziumTopic implements ReactiveSource,TopologyPipeComponent {

	private final SourceMetadata metadata;
	private final ReactiveParameters parameters;
	private boolean materialize;

	public DebeziumTopic(SourceMetadata metadata, ReactiveParameters params) {
		this.metadata = metadata;
		this.parameters = params;
	}
	@Override
	public Flowable<DataItem> execute(StreamScriptContext context, Optional<ImmutableMessage> current,
			ImmutableMessage paramMessage) {
		return Flowable.error(new Exception("Topology sources shouldn't be executed"));
	}
	
	private String assembleDebeziumTopicName(TopologyContext topologyContext, String resource, String schema, String table) {
		return CoreOperators.topicName(resource+"."+schema+"."+table, topologyContext);
		 
	}
	
	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {

		StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);

		
		ReactiveResolvedParameters resolved = parameters.resolve(context, Optional.empty(), ImmutableFactory.empty(), metadata);

		
//		Generic-test-dvd.public.city
		String table = resolved.paramString("table");
		String schema = resolved.paramString("schema");
		String resource = resolved.paramString("resource");
		
		boolean appendTenant = resolved.optionalBoolean("appendTenant").orElse(false);
		boolean appendSchema = resolved.optionalBoolean("appendSchema").orElse(false);
		boolean appendTable = false;
		String name = processorName(createName(topologyContext, transformerNames.size(),pipeId));
		String topicName = assembleDebeziumTopicName(topologyContext,resource,schema,table);
//		CoreOperators.topicName(topic, topologyContext);
		Map<String,String> associatedSettings = new HashMap<>();
		associatedSettings.put("resource", resource);
		associatedSettings.put("schema", schema);
		associatedSettings.put("table", table);
//		associatedSettings.put("topic", topicName);

		topologyConstructor.addConnectSink(resource,topicName, associatedSettings);

		final String sourceProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium_conversion_source")+"-"+topicName;
	    final String convertProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium_conversion")+"-"+topicName;
	    final String finalProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium")+"-"+topicName;
	    ReplicationTopologyParser.addLazySourceStore(topology, topologyContext, topologyConstructor, topicName, Serdes.String().deserializer(),Serdes.ByteArray().deserializer());
	    //topology.addSource(sourceProcessorName,Serdes.String().deserializer(),Serdes.ByteArray().deserializer(), topicName);
//		public static String addLazySourceStore(final Topology currentBuilder, TopologyContext context,
//				TopologyConstructor topologyConstructor, String sourceTopicName, Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
		
//	    Serializer<PubSubMessage> ser = new PubSubSerializer();
		topology.addProcessor(convertProcessorName, ()->new DebeziumConversionProcessor(topicName,topologyContext, appendTenant, appendSchema,appendTable), topicName);

		
//		materialize = true;
		
		if(materialize) {
			topology.addProcessor(finalProcessorName,()->new StoreProcessor(ReplicationTopologyParser.STORE_PREFIX+finalProcessorName), convertProcessorName);
		} else {
			topology.addProcessor(finalProcessorName,()->new IdentityProcessor(), convertProcessorName);
			
		}
		
        if(materialize) {
        	ReplicationTopologyParser.addStateStoreMapping(topologyConstructor.processorStateStoreMapper,finalProcessorName, ReplicationTopologyParser.STORE_PREFIX+finalProcessorName);
    		topologyConstructor.stores.add(ReplicationTopologyParser.STORE_PREFIX+finalProcessorName);
            topologyConstructor.stateStoreSupplier.put(ReplicationTopologyParser.STORE_PREFIX+finalProcessorName,ReplicationTopologyParser.createMessageStoreSupplier(ReplicationTopologyParser.STORE_PREFIX+finalProcessorName,true));
        }
		transformerNames.push(finalProcessorName);
	}

	private static String processorName(String sourceTopicName) {
        return sourceTopicName.replace(':',  '_').replace('@', '.');
    }
	
	private  String createName(TopologyContext topologyContext, int transformerNumber, int pipeId) {
		return topologyContext.instance+"_"+pipeId+"_"+metadata.sourceName()+"_"+transformerNumber;
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
