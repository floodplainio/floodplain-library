package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.debezium.impl.DebeziumConversionProcessor;
import com.dexels.kafka.streams.remotejoin.IdentityProcessor;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.StoreProcessor;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class DebeziumTopic implements TopologyPipeComponent {

	private final String table;
	private final String schema;
	private final String resource;
	private final boolean appendTenant;
	private final boolean appendSchema;
	private boolean materialize;

	public DebeziumTopic(String table, String schema, String resource, boolean appendTenant, boolean appendSchema) {
		this.table = table;
		this.schema = schema;
		this.resource = resource;
		this.appendTenant = appendTenant;
		this.appendSchema = appendSchema;
	}

	
	private String assembleDebeziumTopicName(TopologyContext topologyContext, String resource, String schema, String table) {
		return CoreOperators.topicName(resource+"."+schema+"."+table, topologyContext);
		 
	}
	
	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor, ImmutableMessage stateMessage) {
		boolean appendTable = false;
//		String name = processorName(createName(topologyContext, transformerNames.size(),pipeId));
		final String metadataName = "debezium";
//		String name = topologyContext.qualifiedName(metadataName,transformerNames.size(), pipeId);

		String topicName = assembleDebeziumTopicName(topologyContext,resource,schema,table);
//		CoreOperators.topicName(topic, topologyContext);
		Map<String,String> associatedSettings = new HashMap<>();
		associatedSettings.put("resource", resource);
		associatedSettings.put("schema", schema);
		associatedSettings.put("table", table);
//		associatedSettings.put("topic", topicName);

		topologyConstructor.addConnectSink(resource,topicName, associatedSettings);

		final String sourceProcessorName = topologyContext.qualifiedName(metadataName +"_debsrc",transformerNames.size(), pipeId);
//		final String sourceProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium_conversion_source")+"-"+topicName;
		final String convertProcessorName = topologyContext.qualifiedName(metadataName +"_debconv",transformerNames.size(), pipeId);
//	    final String convertProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium_conversion")+"-"+topicName;
		final String finalProcessorName = topologyContext.qualifiedName(metadataName +"_deb",transformerNames.size(), pipeId);
//	    final String finalProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium")+"-"+topicName;
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
