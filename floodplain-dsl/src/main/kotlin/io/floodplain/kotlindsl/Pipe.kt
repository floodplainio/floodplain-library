package io.floodplain.kotlindsl

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor

class Pipe() {

    private val sources: MutableList<Source> = ArrayList()

    fun addSource(source: Source) {
        sources.add(source)
    }

    fun sources() : List<Source> {
        return sources
    }

    fun render(context: TopologyContext, topologyConstructor: TopologyConstructor) {

    }
}