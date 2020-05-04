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

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.reactive.topology.ReactivePipe;
import io.floodplain.reactive.topology.ReactivePipeParser;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;

public class JoinRemoteTransformer implements TopologyPipeComponent {

    private final ReactivePipe remoteJoin;
    private final boolean isList;
    private final boolean isOptional;
    private boolean materialize = false;
    private final BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor;

    public JoinRemoteTransformer(ReactivePipe remoteJoin, BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor, boolean isList, boolean isOptional) {
        this.remoteJoin = remoteJoin;
        this.keyExtractor = keyExtractor;
        this.isList = isList;
        this.isOptional = isOptional;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {

        Function<ReplicationMessage, String> keyXtr = msg -> {
            return this.keyExtractor.apply(msg.message(), msg.paramMessage().orElse(ImmutableFactory.empty()));
        };
        GroupTransformer.addGroupTransformer(transformerNames, pipeId, topology, topologyContext, topologyConstructor, keyXtr, "joinRemote");

        Optional<String> from = Optional.of(transformerNames.peek());
        Stack<String> pipeStack = new Stack<>();
        ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, topologyConstructor.generateNewStreamId(), pipeStack, remoteJoin, true);
        String with = pipeStack.peek();

        String name = topologyContext.instance + "_" + pipeId + "_" + "joinRemote" + "_" + transformerNames.size();
        ReplicationTopologyParser.addSingleJoinGrouped(topology, topologyContext, topologyConstructor, from.get(), name, Optional.empty(), isList, with, isOptional);
        transformerNames.push(name);
    }

    @Override
    public boolean materializeParent() {
        return true;
    }

    @Override
    public void setMaterialize() {
        this.materialize = true;
    }


    @Override
    public boolean materialize() {
        return this.materialize;
    }

}
