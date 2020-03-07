package com.dexels.kafka.streams.processor.generic;

import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import org.apache.kafka.streams.Topology;

import java.util.Map;

public interface GenericProcessorBuilder {
	public void build(Topology topology, Map<String,String> config, TopologyContext context, StreamConfiguration streamConfig);
}
