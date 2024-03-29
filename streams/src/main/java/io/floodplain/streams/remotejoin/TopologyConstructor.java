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
import io.floodplain.streams.api.Topic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopologyConstructor {

    private static final Logger logger = LoggerFactory.getLogger(TopologyConstructor.class);

    public final Map<String, List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, ImmutableMessage>>> immutableStoreSupplier = new HashMap<>();
    public final Map<String, StoreBuilder<KeyValueStore<String, Long>>> longStoreSupplier = new HashMap<>();
    // TODO: Could be optional, only needed in xml based stream code
    public final Set<String> stores = new HashSet<>();
    public final Set<String> sinks = new HashSet<>();



    /**
     * Key is the topic object, value is the 'name' of the topic. Often the same as the topic name, but makes it possible to
     * sink to the same topic multiple times
     */
    public final Map<Topic, String> sources = new HashMap<>();
    // TODO could race conditions happen? If so, would that be a problem?

    private final Map<Topic, Optional<Integer>> desiredTopics = new HashMap<Topic, Optional<Integer>>();
    private int streamCounter = 1;

    public TopologyConstructor() {
    }

    public void addDesiredTopic(Topic topic, Optional<Integer> partitions) {
        // if requested with specific partition count, don't overwrite
        if (!desiredTopics.containsKey(topic) || partitions.isPresent())
            desiredTopics.put(topic, partitions);
    }

    public Set<Topic> desiredTopicNames() {
        return desiredTopics.keySet();
    }

    public void ensureTopicExists(Topic topicName, Optional<Integer> partitionCount) {
        desiredTopics.put(topicName, partitionCount);
    }

    public void createTopicsAsNeeded(Map<String,Object> settings) {
        Map<String, Object> config = new HashMap<>(settings);
//        config.put("bootstrap.servers", kafkaHosts);
        // TODO remove these?
        config.put("client.id", UUID.randomUUID().toString());
        config.put("cleanup.policy","compact");
        AdminClient adminClient = AdminClient.create(config);
        Set<String> topics = new HashSet<String>();

        logger.info("Required topics: {}",desiredTopics);
        try {
            topics.addAll(adminClient.listTopics().names().get());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error listing topics", e);
        }
        List<NewTopic> toBeCreated = desiredTopics.entrySet()
                .stream()
                .filter(e -> !topics.contains(e.getKey().qualifiedString()))
                .map(e -> createTopic(e.getKey(),e.getValue()))
                .collect(Collectors.toList());
        logger.info("Creating missing topics: {}",toBeCreated.stream().map(e->e.name()).collect(Collectors.joining(",")));
        try {
            adminClient.createTopics(toBeCreated).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Issue creating topics: " + desiredTopics.keySet(), e);
        } finally {
            adminClient.close();
        }
    }

    private NewTopic createTopic(Topic topic, Optional<Integer> partitions) {
        int effectivePartitions = partitions.orElse(1);
        NewTopic result = new NewTopic(topic.qualifiedString(), Optional.of(effectivePartitions), Optional.empty());
        return result.configs(Map.of("cleanup.policy", "compact"));
    }

    public int generateNewStreamId() {
        return streamCounter++;
    }

    public void addSink(String qualifiedName) {
        sinks.add(qualifiedName);
    }
}
