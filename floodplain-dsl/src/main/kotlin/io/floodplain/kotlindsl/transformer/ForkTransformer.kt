package io.floodplain.kotlindsl.transformer

import io.floodplain.kotlindsl.Block
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.streams.Topology
import java.lang.RuntimeException
import java.util.*

class ForkTransformer(val blocks: List<Block>) : TopologyPipeComponent {
    var materialize = false;

    override fun addToTopology(transformerNames: Stack<String>, currentPipeId: Int, topology: Topology, topologyContext: TopologyContext, topologyConstructor: TopologyConstructor) {
 //       val currentTop = transformerNames.peek()
//        if(materialize) {
//            throw RuntimeException("Materialization hasn't been implemented TODO")
//        }
        for (b in blocks) {
            val transformerList = b.transformers.map { e->e.component }.toList()
            // create a new stack, so we're sure it is unchanged:
            val stackCopy: Stack<String> = Stack()
            stackCopy.addAll(transformerNames)
            for( tpc in transformerList) {
                tpc.addToTopology(stackCopy,topologyConstructor.generateNewPipeId(),topology, topologyContext, topologyConstructor)
            }
        }
    }

    override fun materialize(): Boolean {
        return materialize
    }


    override fun materializeParent(): Boolean {
        return false
    }

    override fun setMaterialize() {
        materialize = true
    }

}