package com.dexels.navajo.reactive.source.topology;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.kafka.streams.serializer.ConnectReplicationMessageSerde;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Stack;

import static com.dexels.kafka.streams.api.CoreOperators.topicName;

public class SinkTransformer implements TopologyPipeComponent {

    private final String name;
    private final Optional<Integer> partitions;

    private boolean create = true;

    private boolean materialize = false;


    private final static Logger logger = LoggerFactory.getLogger(SinkTransformer.class);

    public static final String SINK_PREFIX = "SINK_";
    public static final String SINKLOG_PREFIX = "SINK_LOG_";

    public SinkTransformer(String name, Optional<Integer> partitions) {
        this.name = name;
        this.partitions = partitions;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {

        String sinkTopic = topicName(name, topologyContext);
        // TODO shouldn't we use the createName?
        // TODO still weird if we use multiple
        if (create) {
            topologyConstructor.ensureTopicExists(sinkTopic, partitions);
        }
        logger.info("Stack top for transformer: " + transformerNames.peek());
        topology.addSink(SINK_PREFIX + sinkTopic, sinkTopic, transformerNames.peek());
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
