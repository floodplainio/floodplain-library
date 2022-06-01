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

import io.floodplain.immutable.api.ImmutableMessage
import io.floodplain.reactive.source.topology.FlattenProcessor
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.ReplicationTopologyParser
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.TopologyException
import java.util.Stack

class CompareToTransformer(val transform: (String, ImmutableMessage?, ImmutableMessage?) -> ImmutableMessage?) : TopologyPipeComponent {
    var materialize = false

    override fun addToTopology(
        transformerNames: Stack<String>,
        currentPipeId: Int,
        topology: Topology,
        topologyContext: TopologyContext,
        topologyConstructor: TopologyConstructor
    ) {
        val top = transformerNames.peek()
        val name = topologyContext.qualifiedName("compareTo", transformerNames.size, currentPipeId)
        if (materialize) {
            ReplicationTopologyParser.addCompareToProcessor(topology, topologyContext, topologyConstructor, top, name+ "_prematerialize",transform)
            ReplicationTopologyParser.addMaterializeStore(
                topology,
                topologyContext,
                topologyConstructor,
                name,
                name + "_prematerialize"
            )
        } else {
            ReplicationTopologyParser.addCompareToProcessor(topology, topologyContext, topologyConstructor, top, name,transform)
        }
        transformerNames.push(name)
    }

    override fun materializeParent(): Boolean {
        return false
    }

    override fun setMaterialize() {
        materialize = true
    }
}
