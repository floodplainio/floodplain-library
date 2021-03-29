/**
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
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ScanTransformer implements TopologyPipeComponent {

    private final Function<ImmutableMessage, ImmutableMessage> initial;
    private final List<TopologyPipeComponent> onAdd;
    private final List<TopologyPipeComponent> onRemove;
    private boolean materialize;
    private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;

    public ScanTransformer(BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor, Function<ImmutableMessage,ImmutableMessage> initial, List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove) {
        this.keyExtractor = Optional.ofNullable(keyExtractor);
        this.initial = initial;
        this.onAdd = onAdd;
        this.onRemove = onRemove;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        String reducerName = ReplicationTopologyParser.addReducer(topology, topologyContext, topologyConstructor, transformerNames, currentPipeId, onAdd, onRemove, initial, materialize, keyExtractor);
        transformerNames.push(reducerName);
    }

    @Override
    public boolean materializeParent() {
        return false;
    }

    @Override
    public void setMaterialize() {
        this.materialize = true;
    }

}
