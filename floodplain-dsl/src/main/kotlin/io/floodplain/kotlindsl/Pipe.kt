package io.floodplain.kotlindsl

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import com.dexels.navajo.reactive.topology.ReactivePipeParser
import org.apache.kafka.streams.Topology
import java.util.*
import kotlin.collections.ArrayList

class Pipe() {

    private val sources: MutableList<Source> = ArrayList()

    fun addSource(source: Source) {
        sources.add(source)
    }

    fun sources(): List<Source> {
        return sources
    }

    fun render(topology: Topology, context: TopologyContext, topologyConstructor: TopologyConstructor) {
        val reactivePipes = sources.map { e -> e.toReactivePipe() }
        val stack = Stack<String>()
        for (reactivePipe in reactivePipes) {
            ReactivePipeParser.processPipe(context, topologyConstructor, topology, topologyConstructor.generateNewPipeId(), stack, reactivePipe, true)
//
////            public void addToTopology(Stack<String> transformerNames, int currentPipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor);
//            reactivePipe.source.addToTopology(stack,topologyConstructor.generateNewPipeId(),topology,context,topologyConstructor)
//            reactivePipe.transformers.forEach {
//                e->e.addToTopology(stack,topologyConstructor.generateNewPipeId(),topology,context,topologyConstructor)
//            }
        }
        ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology)

    }
}