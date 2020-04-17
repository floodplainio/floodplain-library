package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;
import java.util.function.BiConsumer;

public class EachTransformer implements TopologyPipeComponent {

//    private final BiConsumer<ImmutableMessage, ImmutableMessage> lambda;
    private boolean materialize = false;


    private final static Logger logger = LoggerFactory.getLogger(EachTransformer.class);

    public static final String SINK_PREFIX = "SINK_";
    public static final String SINKLOG_PREFIX = "SINK_LOG_";
    ImmutableMessage.TriConsumer lambda;
    public EachTransformer(ImmutableMessage.TriConsumer lambda) {
        this.lambda = lambda;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
//		boolean dumpStack = resolved.optionalBoolean("dumpStack").orElse(false);
//		if(every.isPresent()) {
//			throw new UnsupportedOperationException("'every' param not yet implemented in LogTransformer");
//		}
//		String logName = resolved.paramString("logName");
        logger.info("Stack top for transformer: " + transformerNames.peek());
//		String name = createName(topologyContext, transformerNames.size(), pipeId);
        String name = topologyContext.qualifiedName("log", transformerNames.size(), pipeId);
        if (this.materialize()) {
//			topology.addProcessor(filterName+"_prematerialize",filterProcessor, transformerNames.peek());
            topology.addProcessor(name + "_prematerialize", () -> new EachProcessor(lambda), transformerNames.peek());
            ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name + "_prematerialize");
        } else {
            topology.addProcessor(name, () -> new EachProcessor(lambda), transformerNames.peek());
        }
        transformerNames.push(name);
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
