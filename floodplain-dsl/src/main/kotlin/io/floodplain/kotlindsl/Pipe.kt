package io.floodplain.kotlindsl

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import com.dexels.navajo.reactive.topology.ReactivePipe

class Pipe() {

    private val sources: MutableList<Source> = ArrayList()

    fun addSource(source: Source) {
        sources.add(source)
    }

    fun sources() : List<Source> {
        return sources
    }

    fun render(context: TopologyContext, topologyConstructor: TopologyConstructor) {
        val reactivePipes =  sources.map { e->e.toReactivePipe() }
        for (reactivePipe in reactivePipes) {
//            reactivePipe.source.addToTopology()
        }
    }
}