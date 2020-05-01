package io.floodplain.reactive.source.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Stack;

import static io.floodplain.streams.api.CoreOperators.topicName;

public class SinkTransformer implements TopologyPipeComponent {

    private final String name;
    private final Optional<Integer> partitions;
    private final boolean materializeParent;

    private boolean create = true;

    private boolean materialize = false;


    private final static Logger logger = LoggerFactory.getLogger(SinkTransformer.class);

    public static final String SINK_PREFIX = "SINK_";

    public SinkTransformer(String name, boolean materializeParent, Optional<Integer> partitions) {
        this.name = name;
        this.partitions = partitions;
        this.materializeParent = materializeParent;
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
        return materializeParent;
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
