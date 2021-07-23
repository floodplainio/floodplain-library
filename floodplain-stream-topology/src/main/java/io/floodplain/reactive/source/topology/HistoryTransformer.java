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
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.HistoryProcessor;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

import static io.floodplain.streams.remotejoin.ReplicationTopologyParser.STORE_PREFIX;

public class HistoryTransformer implements TopologyPipeComponent {

    private boolean materialized = false;
    private final static Logger logger = LoggerFactory.getLogger(HistoryTransformer.class);


    public HistoryTransformer() {
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        String historyName = topologyContext.qualifiedName("history", transformerNames.size(), pipeId);
        String historyKeyCountName = topologyContext.qualifiedName("historykeycount", transformerNames.size(), pipeId);
        String historyStoreName = STORE_PREFIX+historyName;
        String historyKeyCountStoreName = STORE_PREFIX+""+historyKeyCountName;
        ReplicationTopologyParser.addStateStoreMapping(topologyConstructor.processorStateStoreMapper, historyName, historyStoreName);
        ReplicationTopologyParser.addStateStoreMapping(topologyConstructor.processorStateStoreMapper, historyName, historyKeyCountStoreName);

        logger.info("Granting access for processor: {} to store: {}", historyName, historyStoreName);
        topologyConstructor.stateStoreSupplier.put(historyStoreName, ReplicationTopologyParser.createMessageStoreSupplier(historyStoreName, true));
        topologyConstructor.longStoreSupplier.put(historyKeyCountStoreName, ReplicationTopologyParser.createLongStoreSupplier(historyKeyCountStoreName, true));

        logger.info("Stack top for transformer: {}", transformerNames.peek());
        if (this.materialized) {
            topology.addProcessor(historyName, () -> new HistoryProcessor(historyStoreName,historyKeyCountStoreName), transformerNames.peek());
            ReplicationTopologyParser.addMaterializeStore(topology, topologyContext, topologyConstructor, historyName, historyName + "_prematerialize");
        } else {
            topology.addProcessor(historyName, () -> new HistoryProcessor(historyStoreName,historyKeyCountStoreName), transformerNames.peek());
        }
        transformerNames.push(historyName);
    }

    @Override
    public boolean materializeParent() {
        return false;
    }

    @Override
    public void setMaterialize() {
        this.materialized = true;
    }


}
