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
package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopologyConstructor {

    public final Map<String, List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ImmutableMessage>>> immutableStoreSupplier = new HashMap<>();
    // TODO: Could be optional, only needed in xml based stream code
    public final Set<String> stores = new HashSet<>();
    public final Map<String, String> sources = new HashMap<>();
    // TODO could race conditions happen? If so, would that be a problem?

    private final Map<String, Optional<Integer>> desiredTopics = new HashMap<>();
    private int streamCounter = 1;

    public TopologyConstructor() {
    }

    public void addDesiredTopic(String topicName, Optional<Integer> partitions) {
        // if requested with specific partition count, don't overwrite
        if (!desiredTopics.containsKey(topicName) || partitions.isPresent())
            desiredTopics.put(topicName, partitions);
    }

    public void ensureTopicExists(String topicName, Optional<Integer> partitionCount) {
        desiredTopics.put(topicName, partitionCount);
    }

    public void createTopicsAsNeeded(String kafkaHosts) {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", kafkaHosts);
        config.put("client.id", UUID.randomUUID().toString());

        AdminClient adminClient = AdminClient.create(config);
        Set<String> topics = new HashSet<String>();

        try {
            topics.addAll(adminClient.listTopics().names().get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error listing topics", e);
        }
        List<NewTopic> toBeCreated = desiredTopics.entrySet()
                .stream()
                .filter(e -> !topics.contains(e.getKey()))
                .map(e -> new NewTopic(e.getKey(), e.getValue(), Optional.empty()))
                .collect(Collectors.toList());
        try {
            adminClient.createTopics(toBeCreated).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Issue creating topics: " + desiredTopics.keySet(), e);
        }
    }

    public int generateNewStreamId() {
        return streamCounter++;
    }

}
