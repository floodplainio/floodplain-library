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

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.ReplicationTopologyParser.addDiffProcessor
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.streams.Topology
import java.util.Stack

class DiffTransformer : TopologyPipeComponent {
    var materialize = false
    override fun materialize(): Boolean {
        return materialize
    }

    override fun addToTopology(transformerNames: Stack<String>, currentPipeId: Int, topology: Topology, topologyContext: TopologyContext, topologyConstructor: TopologyConstructor) {
        if (materialize) {
            throw RuntimeException("Materialization hasn't been implemented TODO")
        }
        val top = transformerNames.peek()
        val name = topologyContext.qualifiedName("diff", transformerNames.size, currentPipeId)
        addDiffProcessor(topology, topologyContext, topologyConstructor, top, name)
        transformerNames.push(name)
    }

    override fun materializeParent(): Boolean {
        return false
    }

    override fun setMaterialize() {
        materialize = true
    }
}
