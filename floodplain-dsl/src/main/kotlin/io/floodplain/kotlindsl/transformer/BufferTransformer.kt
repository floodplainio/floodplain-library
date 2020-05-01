package io.floodplain.kotlindsl.transformer

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.ReplicationTopologyParser.addMaterializeStore
import io.floodplain.streams.remotejoin.ReplicationTopologyParser.addPersistentCache
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.streams.Topology
import java.time.Duration
import java.util.*

class BufferTransformer(val duration: Duration, val maxSize: Int, val inMemory: Boolean): TopologyPipeComponent {
    var materialize = false
    override fun materialize(): Boolean {
        return materialize
    }

    override fun addToTopology(transformerNames: Stack<String>, currentPipeId: Int, topology: Topology, topologyContext: TopologyContext, topologyConstructor: TopologyConstructor) {
        if(materialize) {
            throw RuntimeException("Materialization hasn't been implemented TODO")
        }
        var top = transformerNames.peek()
        val name = topologyContext.qualifiedName("buffer", transformerNames.size, currentPipeId)
        if (materialize) {
            val prematerialize = topologyContext.qualifiedName("buffer-prematerialize", transformerNames.size, currentPipeId)
            addPersistentCache(topology,topologyContext,topologyConstructor,name,top,duration,maxSize,inMemory)
            addMaterializeStore(topology, topologyContext, topologyConstructor, name, name + "_prematerialize")

        } else {
            addPersistentCache(topology,topologyContext,topologyConstructor,name,top,duration,maxSize,inMemory)
            transformerNames.push(name)

        }
    }

    override fun materializeParent(): Boolean {
        return false
    }

    override fun setMaterialize() {
        materialize = true
    }
}