package com.dexels.navajo.reactive.source.topology;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.debezium.impl.DebeziumConversionProcessor;
import com.dexels.kafka.streams.remotejoin.IdentityProcessor;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.StoreProcessor;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class DebeziumTopicSource implements TopologyPipeComponent {

    private final String table;
    private final String schema;
    private final String resource;
    private final boolean appendTenant;
    private final boolean appendSchema;
    private boolean materialize;

    public DebeziumTopicSource(String resource, String table, String schema, boolean appendTenant, boolean appendSchema) {
        this.resource = resource;
        this.table = table;
        this.schema = schema;
        this.appendTenant = appendTenant;
        this.appendSchema = appendSchema;
    }


    public String topicName(TopologyContext topologyContext) {
        return CoreOperators.topicName(resource + "." + schema + "." + table, topologyContext);

    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        boolean appendTable = false;
        final String metadataName = "debezium";

        String topicName = topicName(topologyContext);


//        topologyConstructor.addConnectSink(resource, topicName, associatedSettings);

//        final String sourceProcessorName = topologyContext.qualifiedName(metadataName + "_debsrc", transformerNames.size(), pipeId);
        final String convertProcessorName = topologyContext.qualifiedName(metadataName + "_debconv", transformerNames.size(), pipeId);
        final String finalProcessorName = topologyContext.qualifiedName(metadataName + "_deb", transformerNames.size(), pipeId);
        ReplicationTopologyParser.addLazySourceStore(topology, topologyContext, topologyConstructor, topicName, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        topology.addProcessor(convertProcessorName, () -> new DebeziumConversionProcessor(topicName, topologyContext, appendTenant, appendSchema, appendTable), topicName);

        if (materialize) {
            topology.addProcessor(finalProcessorName, () -> new StoreProcessor(ReplicationTopologyParser.STORE_PREFIX + finalProcessorName), convertProcessorName);
        } else {
            topology.addProcessor(finalProcessorName, () -> new IdentityProcessor(), convertProcessorName);

        }

        if (materialize) {
            ReplicationTopologyParser.addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalProcessorName, ReplicationTopologyParser.STORE_PREFIX + finalProcessorName);
            topologyConstructor.stores.add(ReplicationTopologyParser.STORE_PREFIX + finalProcessorName);
            topologyConstructor.stateStoreSupplier.put(ReplicationTopologyParser.STORE_PREFIX + finalProcessorName, ReplicationTopologyParser.createMessageStoreSupplier(ReplicationTopologyParser.STORE_PREFIX + finalProcessorName, true));
        }
        transformerNames.push(finalProcessorName);
    }
//
//    private static String processorName(String sourceTopicName) {
//        return sourceTopicName.replace(':', '_').replace('@', '.');
//    }

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
