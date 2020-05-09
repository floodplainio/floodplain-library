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
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;

public class DynamicSinkTransformer implements TopologyPipeComponent {

    private final Optional<Integer> partitions;
    private final String sinkName;

    private boolean materialize = false;


    private final static Logger logger = LoggerFactory.getLogger(DynamicSinkTransformer.class);

    public static final String SINK_PREFIX = "SINK_";

    private final BiFunction<String,ImmutableMessage,String> extractor;

    public DynamicSinkTransformer(String sinkName, Optional<Integer> partitions, BiFunction<String,ImmutableMessage,String> extractor) {
        this.extractor = extractor;
        this.partitions = partitions;
        this.sinkName = sinkName;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        TopicNameExtractor<String, ReplicationMessage> topicNameExtractor = (key, msg, recordContext)->topologyContext.topicName( extractor.apply(key, msg.message()));
        logger.info("Stack top for transformer: " + transformerNames.peek());
        topology.addSink(SINK_PREFIX + sinkName, topicNameExtractor, transformerNames.peek());
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
