package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;
import java.util.function.BiFunction;

public class FilterTransformer implements TopologyPipeComponent {

    private final ProcessorSupplier<String, ReplicationMessage> filterProcessor;
    private boolean materialized = false;

    private final static Logger logger = LoggerFactory.getLogger(FilterTransformer.class);


    public FilterTransformer(BiFunction<ImmutableMessage, ImmutableMessage, Boolean> func) {

        this.filterProcessor = () -> new FilterProcessor(func);
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
//		String filterName = createName(topologyContext.instance, transformerNames.size(), pipeId);
        String filterName = topologyContext.qualifiedName("filter", transformerNames.size(), pipeId);

        logger.info("Stack top for transformer: {}", transformerNames.peek());
        if (this.materialized) {
            topology.addProcessor(filterName + "_prematerialize", filterProcessor, transformerNames.peek());
            ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, filterName, filterName + "_prematerialize");
        } else {
            topology.addProcessor(filterName, filterProcessor, transformerNames.peek());
        }
        transformerNames.push(filterName);
    }

    @Override
    public boolean materializeParent() {
        return false;
    }

    @Override
    public void setMaterialize() {
        this.materialized = true;
    }

    @Override
    public boolean materialize() {
        return this.materialized;
    }

}
