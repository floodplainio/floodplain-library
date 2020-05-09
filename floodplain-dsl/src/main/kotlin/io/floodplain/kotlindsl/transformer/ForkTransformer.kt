/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.kotlindsl.transformer

import io.floodplain.kotlindsl.Block
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import java.util.Stack
import org.apache.kafka.streams.Topology

// Implemented this one in Kotlin (most are in Java, just to see if there were any complications, there seem to be none
class ForkTransformer(val blocks: List<Block>) : TopologyPipeComponent {
    var materialize = false

    override fun addToTopology(transformerNames: Stack<String>, currentPipeId: Int, topology: Topology, topologyContext: TopologyContext, topologyConstructor: TopologyConstructor) {
        if (materialize) {
            throw RuntimeException("Materialization hasn't been implemented TODO")
        }
        for (b in blocks) {
            val transformerList = b.transformers.map { e -> e.component }.toList()
            // create a new stack, so we're sure it is unchanged:
            val stackCopy: Stack<String> = Stack()
            stackCopy.addAll(transformerNames)
            for (tpc in transformerList) {
                tpc.addToTopology(stackCopy, topologyConstructor.generateNewStreamId(), topology, topologyContext, topologyConstructor)
            }
        }
    }

    override fun materializeParent(): Boolean {
        return false
    }

    override fun setMaterialize() {
        materialize = true
    }
}
