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

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.streams.api.Topic;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import io.floodplain.streams.serializer.ReplicationMessageSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;

public class CustomTopicSource implements TopologyPipeComponent {

    private final Topic topic;
    private final Deserializer<String> keyFormat;
    private final Deserializer<ReplicationMessage> bodyFormat;
    private static final ReplicationMessageSerde replicationMessageSerde = new ReplicationMessageSerde();

    private boolean materialize = false;

    public CustomTopicSource(Topic topic, Function<byte[],String> keyExtractor, Function<byte[], ReplicationMessage> bodyExtractor) {
        this.topic = topic;
        this.keyFormat = (tp, data) -> keyExtractor.apply(data);
        this.bodyFormat = (tp,data) -> bodyExtractor.apply(data);
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int pipeId, Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
        String source = ReplicationTopologyParser.addSourceStore(topology, topologyConstructor, topic,keyFormat,bodyFormat, this.materialize);
        topologyConstructor.addDesiredTopic(topic, Optional.empty());
        transformerNames.push(source);
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
