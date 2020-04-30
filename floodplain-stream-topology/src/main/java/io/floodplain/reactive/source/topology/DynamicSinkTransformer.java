package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;

import static io.floodplain.streams.api.CoreOperators.topicName;

public class DynamicSinkTransformer implements TopologyPipeComponent {

    private final Optional<Integer> partitions;
    private final String sinkName;

    private boolean create = true;

    private boolean materialize = false;


    private final static Logger logger = LoggerFactory.getLogger(DynamicSinkTransformer.class);

    public static final String SINK_PREFIX = "SINK_";

    private final BiFunction<String,ImmutableMessage,String> extractor;

    public DynamicSinkTransformer(String sinkName, Optional<Integer> partitions, BiFunction<String,ImmutableMessage,String> extractor) {
        this.extractor = extractor;
        this.partitions = partitions;
        this.sinkName = sinkName;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        TopicNameExtractor<String, ReplicationMessage> topicNameExtractor = (key, msg, recordContext)->topicName( extractor.apply(key, msg.message()),topologyContext);

//        String sinkTopic = topicName(name, topologyContext);
        // TODO shouldn't we use the createName?
        // TODO still weird if we use multiple
//        if (create) {
//            topologyConstructor.ensureTopicExists(sinkTopic, partitions);
//        }
        logger.info("Stack top for transformer: " + transformerNames.peek());
        topology.addSink(SINK_PREFIX + sinkName, topicNameExtractor, transformerNames.peek());
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
