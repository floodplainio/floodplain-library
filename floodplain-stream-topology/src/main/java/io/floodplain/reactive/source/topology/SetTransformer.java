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
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;
import java.util.function.BiFunction;

public class SetTransformer implements TopologyPipeComponent {

    private boolean materialize;

    public static interface TriFunction {
        ImmutableMessage apply(String key, ImmutableMessage primary, ImmutableMessage secondary);
    }

    //	private final boolean fromEmpty;
    private final TriFunction transformer;


    private final static Logger logger = LoggerFactory.getLogger(SetTransformer.class);
    public SetTransformer(TriFunction transformer) {
        this.transformer = transformer;
    }


    @Override
    public void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        FunctionProcessor fp = new FunctionProcessor(this.transformer);
        String name = topologyContext.qualifiedName("set", transformerNames.size(), currentPipeId);
        logger.info("Adding processor: {} to parent: {} hash: {}", name, transformerNames, transformerNames.hashCode());
        if (this.materialize()) {
            topology.addProcessor(name + "_prematerialize", () -> fp, transformerNames.peek());
            ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, name, name + "_prematerialize");
        } else {
            topology.addProcessor(name, () -> fp, transformerNames.peek());
        }
        transformerNames.push(name);
    }

    @Override
    public boolean materializeParent() {
        return false;
    }

    @Override
    public void setMaterialize() {
        this.materialize = true;
    }

    @Override
    public boolean materialize() {
        return materialize;
    }


}
