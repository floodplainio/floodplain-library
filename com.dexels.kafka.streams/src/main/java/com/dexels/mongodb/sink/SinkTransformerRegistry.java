package com.dexels.mongodb.sink;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SinkTransformerRegistry {

	private static final Map<String,MessageTransformer> transformers = new HashMap<>();
	
	private SinkTransformerRegistry() {
		// no instances
	}

	// Ugly ids (generally include tenant, deployment etc. twice) but only 'visible' in the map
	public static Optional<MessageTransformer> transformerForSink( String name, TopologyContext context, String topic) {
		String sinkId = CoreOperators.generationalGroup(name, context)+"-"+topic;
		return Optional.ofNullable(transformers.get(sinkId));
	}
	
	public static void registerTransformerForSink(String name, TopologyContext topologyContext, String topic, MessageTransformer transformer) {
		String sinkId = CoreOperators.generationalGroup(name, topologyContext)+"-"+topic;
		transformers.put(sinkId, transformer);
	}
}
