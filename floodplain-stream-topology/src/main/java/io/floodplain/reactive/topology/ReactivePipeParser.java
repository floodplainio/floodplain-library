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
package io.floodplain.reactive.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class ReactivePipeParser {


    private final static Logger logger = LoggerFactory.getLogger(ReactivePipeParser.class);

    public static void processPipe(TopologyContext topologyContext, TopologyConstructor topologyConstructor, Topology topology,
                                   int pipeNr, Stack<String> pipeStack, ReactivePipe pipe, boolean materializeTop) {
        int size = pipe.transformers.size();
        if (size == 0) {
            // no transformers
            if (materializeTop) {
                TopologyPipeComponent source = pipe.source;
                source.setMaterialize();
            }
        } else {
            if (materializeTop) {
                TopologyPipeComponent top = pipe.transformers.get(size - 1);
                top.setMaterialize();
            }
        }
        for (int i = size; i >= 0; i--) {
            TopologyPipeComponent source = pipe.source;
            if (i == 0) {
                logger.info("processing source");
            } else {
                TopologyPipeComponent type = pipe.transformers.get(i - 1);
                TopologyPipeComponent parent = i - 2 < 0 ? source : pipe.transformers.get(i - 2);
                logger.info("processing transformer: " + (i - 1));
                if (type.materializeParent()) {
                    logger.info("Materializing parent");
                    parent.setMaterialize();
                }
            }
        }

        TopologyPipeComponent sourceTopologyComponent = pipe.source;
        sourceTopologyComponent.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
        for (Object e : pipe.transformers) {
            logger.info("Transformer: {} pipestack: {}", e, pipeStack);
            if (e instanceof TopologyPipeComponent) {
                TopologyPipeComponent tpc = (TopologyPipeComponent) e;
                logger.info("Adding pipe component: " + tpc.getClass() + " to stack: " + pipeStack);
                tpc.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
            }
        }
    }

}
