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
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GroupTransformer implements TopologyPipeComponent {

    private final BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor;

    public GroupTransformer(BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        Function<ReplicationMessage, String> keyExtractor = msg -> this.keyExtractor.apply(msg.message(), msg.paramMessage().orElse(ImmutableFactory.empty()));

        addGroupTransformer(transformerNames, pipeId, topology, topologyContext, topologyConstructor, keyExtractor, "group");

    }

    public static void addGroupTransformer(Stack<String> transformerNames, int pipeId, Topology topology,
                                           TopologyContext topologyContext, TopologyConstructor topologyConstructor, Function<ReplicationMessage, String> keyExtractor, String transformerName) {
        String from = transformerNames.peek();
        String name = topologyContext.qualifiedName(transformerName, transformerNames.size(), pipeId);
        String grouped = ReplicationTopologyParser.addGroupedProcessor(topology, topologyContext, topologyConstructor, name, from, keyExtractor, Optional.empty());
        transformerNames.push(grouped);
    }


    @Override
    public boolean materializeParent() {
        return true;
    }

    @Override
    public void setMaterialize() {
        // no-op, a group is always materialized
    }

}
