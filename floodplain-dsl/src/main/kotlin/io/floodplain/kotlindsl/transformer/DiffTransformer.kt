package io.floodplain.kotlindsl.transformer

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.DiffProcessor
import io.floodplain.streams.remotejoin.ReplicationTopologyParser.STORE_PREFIX
import io.floodplain.streams.remotejoin.ReplicationTopologyParser.addDiffProcessor
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.ProcessorSupplier
import java.util.*

class DiffTransformer: TopologyPipeComponent {
    var materialize = false
    override fun materialize(): Boolean {
        return materialize
    }

    override fun addToTopology(transformerNames: Stack<String>, currentPipeId: Int, topology: Topology, topologyContext: TopologyContext, topologyConstructor: TopologyConstructor) {
        var top = transformerNames.peek()
        val name = topologyContext.qualifiedName("diff", transformerNames.size, currentPipeId)

//        public static void addDiffProcessor(Topology current, TopologyContext context,
//        TopologyConstructor topologyConstructor, String fromProcessor,
//        String diffProcessorNamePrefix) {
        addDiffProcessor(topology,topologyContext,topologyConstructor,top,name)
        transformerNames.push(name)
//        topology.addProcessor(name, ProcessorSupplier<String, ReplicationMessage> {DiffProcessor(STORE_PREFIX + name)}, top)
    }

    override fun materializeParent(): Boolean {
        return false
    }

    override fun setMaterialize() {
        materialize = true
    }
}