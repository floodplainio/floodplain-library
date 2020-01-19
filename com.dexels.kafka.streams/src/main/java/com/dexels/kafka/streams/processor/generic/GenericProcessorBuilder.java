package com.dexels.kafka.streams.processor.generic;

import java.util.Map;

import org.apache.kafka.streams.Topology;

import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;

public interface GenericProcessorBuilder {
	public void build(Topology topology, Map<String,String> config, TopologyContext context, StreamConfiguration streamConfig);
}
