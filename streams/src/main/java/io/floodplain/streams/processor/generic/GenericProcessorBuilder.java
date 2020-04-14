package io.floodplain.streams.processor.generic;

import io.floodplain.streams.api.StreamConfiguration;
import io.floodplain.streams.api.TopologyContext;
import org.apache.kafka.streams.Topology;

import java.util.Map;

public interface GenericProcessorBuilder {
    public void build(Topology topology, Map<String, String> config, TopologyContext context, StreamConfiguration streamConfig);
}
