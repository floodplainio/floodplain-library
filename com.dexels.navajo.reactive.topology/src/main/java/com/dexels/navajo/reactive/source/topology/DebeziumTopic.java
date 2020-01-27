package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.debezium.impl.DebeziumConversionProcessor;
import com.dexels.kafka.streams.debezium.impl.PubSubSerializer;
import com.dexels.kafka.streams.debezium.impl.PubSubTopicNameExtractor;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.SourceMetadata;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.pubsub.rx2.api.PubSubMessage;

import io.reactivex.Flowable;

public class DebeziumTopic implements ReactiveSource,TopologyPipeComponent {

	private final SourceMetadata metadata;
	private final ReactiveParameters parameters;

	public DebeziumTopic(SourceMetadata metadata, ReactiveParameters params) {
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
		String topic = resolved.paramString("topic");
		boolean appendTenant = resolved.optionalBoolean("appendTenant").orElse(false);
		boolean appendSchema = resolved.optionalBoolean("appendSchema").orElse(false);
//		DebeziumConversionProcessor processor = new DebeziumConversionProcessor(topic, topologyContext, appendTenant, appendSchema);
//		String from = transformerNames.peek();
		String name = processorName(createName(transformerNames.size(),pipeId));
		String topicName = CoreOperators.topicName(topic, topologyContext);
//		topology.addSource("debeziumSource", topicName);
	    final String sourceProcessorName = processorName(name+"_debezium_conversion_source");
//	    final String convertProcessorName = processorName(name+"_debezium_conversion");
//	    final String sinkProcessorName = processorName(name+"_debezium_conversion_sink");
		topology.addSource(sourceProcessorName,Serdes.String().deserializer(),Serdes.ByteArray().deserializer(), topicName);
		
//		topology.addProcessor(name, ()->processor, "debeziumSource");
	    Serializer<PubSubMessage> ser = new PubSubSerializer();
	    
//		Serd
		topology.addProcessor(name, ()->new DebeziumConversionProcessor(topicName,topologyContext, appendTenant, appendSchema), sourceProcessorName);
//		topology.addSink(sinkProcessorName, new PubSubTopicNameExtractor(topologyConstructor),Serdes.String().serializer(), ser, convertProcessorName);
		
//		String sourc2 = ReplicationTopologyParser.addSourceStore(topology, topologyContext, topologyConstructor, Optional.empty(), name, Optional.empty());
//		topology.addProcessor(filterName, filterProcessor, transformerNames.peek());
		System.err.println(">>> "+name);
		transformerNames.push(name);
		return pipeId;
}
	
	
//    String sourceTopic = topicName(source, context);
//    final String sourceProcessorName = processorName(sourceTopic+"_debezium_conversion_source");
//    final String convertProcessorName = processorName(sourceTopic+"_debezium_conversion");
//    final String sinkProcessorName = processorName(sourceTopic+"_debezium_conversion_sink");

	
	private static String processorName(String sourceTopicName) {
        return sourceTopicName.replace(':',  '_').replace('@', '.');
    }
	
	private  String createName(int transformerNumber, int pipeId) {
		return pipeId+"_"+metadata.sourceName()+"_"+transformerNumber;
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
		// ignore
	}
	@Override
	public boolean materialize() {
		return false;
	}

}
