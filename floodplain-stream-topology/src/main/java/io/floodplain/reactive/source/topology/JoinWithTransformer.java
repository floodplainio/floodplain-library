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
package io.floodplain.reactive.source.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.reactive.topology.ReactivePipe;
import io.floodplain.reactive.topology.ReactivePipeParser;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;

public class JoinWithTransformer implements TopologyPipeComponent {

    private final ReactivePipe joinWith;
    private final boolean isOptional;
    private final boolean multiple;
    private final boolean debug;
    private boolean materialize = false;

    public JoinWithTransformer(boolean isOptional, boolean multiple, ReactivePipe joinWith, boolean debug) {
        this.isOptional = isOptional;
        this.joinWith = joinWith;
        this.multiple = multiple;
        this.debug = debug;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        Optional<String> from = Optional.of(transformerNames.peek());
        Stack<String> pipeStack = new Stack<>();
        ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewStreamId(), pipeStack, joinWith, true);
        String top = pipeStack.peek();
        String name = topologyContext.qualifiedName("joinWith", transformerNames.size(), pipeId);
        ReplicationTopologyParser.addJoin(topology, topologyContext, topologyConstructor, from.get(), top, name, isOptional, multiple, this.materialize,this.debug);
        transformerNames.push(name);
    }

    //
    @Override
    public boolean materializeParent() {
        return true;
    }

    @Override
    public void setMaterialize() {
        this.materialize = true;
    }

}
