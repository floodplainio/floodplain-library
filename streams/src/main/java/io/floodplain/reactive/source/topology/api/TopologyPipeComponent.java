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
package io.floodplain.reactive.source.topology.api;

import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Stack;

/**
 * This interface indicates a 'thing that can modify the topology'
 * If the addToTopology gets called, it can add sources, sinks and processors to the Kafka Streams topology
 */
public interface TopologyPipeComponent {
    /**
     * @param transformerNames The list of transformers in the current stream (so in general it is empty for sources)
     * @param currentPipeId Every time start a new (sub) stream, it needs to get a new pipe id.
     * @param topology The topology we are using right now
     * @param topologyContext Immutable datastructure for this run.
     * @param topologyConstructor Mutable state for topology
     */
    void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor);

    /**
     * Returns
     * @return true if this component requires its parent transformer to be materialized.
     */
    boolean materializeParent();

    /**
     * Call this to indicate that when this component will be added to the topology
     */

    void setMaterialize();

}
