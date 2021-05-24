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
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import io.floodplain.streams.api.Topic;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ranged.GroupedUpdateProcessor;
import io.floodplain.streams.remotejoin.ranged.ManyToManyGroupedProcessor;
import io.floodplain.streams.remotejoin.ranged.ManyToOneGroupedProcessor;
import io.floodplain.streams.remotejoin.ranged.OneToManyGroupedProcessor;
import io.floodplain.streams.serializer.ConnectReplicationMessageSerde;
import io.floodplain.streams.serializer.ImmutableMessageSerde;
import io.floodplain.streams.serializer.ReplicationMessageSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ReplicationTopologyParser {
    public static final String STORE_PREFIX = "STORE_";

    private static final Serde<ReplicationMessage> messageSerde = new ReplicationMessageSerde();
    private static final Serde<ImmutableMessage> immutableMessageSerde = new ImmutableMessageSerde();

    private static final ReplicationMessageSerde replicationMessageSerde = new ReplicationMessageSerde();
    private static final ConnectReplicationMessageSerde connectReplicationMessageSerde = new ConnectReplicationMessageSerde();

    public enum Flatten {FIRST, LAST, NONE}

    private static final Logger logger = LoggerFactory.getLogger(ReplicationTopologyParser.class);

    private ReplicationTopologyParser() {
        // - no instances
    }


    public static void addStateStoreMapping(Map<String, List<String>> processorStateStoreMapper, String processor, String stateStore) {
        logger.info("Adding processor: {} with statestore: {}", processor, stateStore);
        List<String> parts = processorStateStoreMapper.computeIfAbsent(stateStore, k -> new ArrayList<>());
        parts.add(processor);
    }

    public static void materializeStateStores(TopologyConstructor topologyConstructor, Topology current) {
        for (Entry<String, List<String>> element : topologyConstructor.processorStateStoreMapper.entrySet()) {
            final String key = element.getKey();
            final StoreBuilder<KeyValueStore<String, ReplicationMessage>> supplier = topologyConstructor.stateStoreSupplier.get(key);
            if (supplier == null) {
                final StoreBuilder<KeyValueStore<String, ImmutableMessage>> immutableSupplier = topologyConstructor.immutableStoreSupplier.get(key);
                if (immutableSupplier != null) {
                    current = current.addStateStore(immutableSupplier, element.getValue().toArray(new String[]{}));
                    logger.info("Added processor: {} with sttstatestores: {} mappings: {}", element.getKey(), element.getValue(), topologyConstructor.processorStateStoreMapper.get(element.getKey()));
                } else {
                    logger.error("Missing supplier for: {}\nStore mappings: {} available suppliers: {}", element.getKey(), topologyConstructor.processorStateStoreMapper, topologyConstructor.immutableStoreSupplier);
                    logger.error("Available state stores: {}\nimm: {}", topologyConstructor.stateStoreSupplier.keySet(), topologyConstructor.immutableStoreSupplier.keySet());
                    throw new RuntimeException("Missing supplier for: " + element.getKey());
                }
            } else {
                current = current.addStateStore(supplier, element.getValue().toArray(new String[]{}));
                logger.info("Added processor: {} with sttstatestores: {} mappings: {}", element.getKey(), element.getValue(), topologyConstructor.processorStateStoreMapper.get(element.getKey()));
            }
        }
    }

    // I think we need to use the topologyContext
    public static void addDiffProcessor(Topology current, TopologyContext topologyContext,
                                        TopologyConstructor topologyConstructor, String fromProcessor,
                                        String diffProcessorNamePrefix) {
        current.addProcessor(diffProcessorNamePrefix, () -> new DiffProcessor(diffProcessorNamePrefix), fromProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, diffProcessorNamePrefix, diffProcessorNamePrefix);
        logger.info("Granting access for processor: {} to store: {}", diffProcessorNamePrefix, diffProcessorNamePrefix);
        topologyConstructor.stateStoreSupplier.put(diffProcessorNamePrefix, createMessageStoreSupplier(diffProcessorNamePrefix, true));
    }

    public static String addMaterializeStore(final Topology currentBuilder, TopologyContext context,
                                             TopologyConstructor topologyConstructor, String name, String parentProcessor) {
        final String sourceProcessorName = name;
        currentBuilder.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + sourceProcessorName), parentProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, sourceProcessorName, STORE_PREFIX + sourceProcessorName);
        topologyConstructor.stores.add(STORE_PREFIX + sourceProcessorName);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + sourceProcessorName, createMessageStoreSupplier(STORE_PREFIX + sourceProcessorName, true));
        return name;
    }

    public static Deserializer<String> keyDeserializer(Topic.FloodplainKeyFormat keyFormat) {
        switch (keyFormat) {

            case CONNECT_KEY_JSON:
                return ConnectReplicationMessageSerde.keyDeserialize();
            case FLOODPLAIN_STRING:
                return Serdes.String().deserializer();
        }
        throw new IllegalArgumentException("Weird key format: " + keyFormat);
    }

    public static Serializer<String> keySerializer(Topic.FloodplainKeyFormat keyFormat) {
        switch (keyFormat) {
            case CONNECT_KEY_JSON:
                return ConnectReplicationMessageSerde.keySerialize();
            case FLOODPLAIN_STRING:
                return Serdes.String().serializer();
        }
        throw new IllegalArgumentException("Weird key format: " + keyFormat);
    }

    public static Deserializer<ReplicationMessage> bodyDeserializer(Topic.FloodplainBodyFormat bodyFormat) {
        switch (bodyFormat) {
            case CONNECT_JSON:
                return connectReplicationMessageSerde.deserializer();
            case FLOODPLAIN_JSON:
                return replicationMessageSerde.deserializer();
        }
        throw new IllegalArgumentException("Weird body format: " + bodyFormat);
    }

    public static Serializer<ReplicationMessage> bodySerializer(Topic.FloodplainBodyFormat bodyFormat) {
        switch (bodyFormat) {
            case CONNECT_JSON:
                return connectReplicationMessageSerde.serializer();
            case FLOODPLAIN_JSON:
                return replicationMessageSerde.serializer();
        }
        throw new IllegalArgumentException("Weird body format: " + bodyFormat);
    }


    public static String addSourceStore(final Topology currentBuilder, TopologyConstructor topologyConstructor, Topic sourceTopicName, Topic.FloodplainKeyFormat keyFormat, Topic.FloodplainBodyFormat bodyFormat, boolean materializeStore) {
        return addSourceStore(currentBuilder,topologyConstructor,sourceTopicName,keyDeserializer(keyFormat), bodyDeserializer(bodyFormat),materializeStore);
    }

    public static String addSourceStore(final Topology currentBuilder, TopologyConstructor topologyConstructor, Topic sourceTopicName, Deserializer<String> keyDeserializer, Deserializer<ReplicationMessage> bodyDeserializer, boolean materializeStore) {
        // TODO It might be better to fail if the topic does not exist? -> Well depends, if it is external yes, but if it is created by the same instance, then no.
        final String sourceProcessorName = sourceTopicName.prefixedString("SOURCE");
        String sourceName;
        if (!topologyConstructor.sources.containsKey(sourceTopicName)) {
            sourceName = sourceProcessorName + "_src";
            currentBuilder.addSource(sourceName, keyDeserializer, bodyDeserializer, sourceTopicName.qualifiedString());
            topologyConstructor.sources.put(sourceTopicName, sourceName);
            if (materializeStore) {
                currentBuilder.addProcessor(sourceProcessorName, () -> new StoreProcessor(STORE_PREFIX + sourceProcessorName), sourceName);
            } else {
                currentBuilder.addProcessor(sourceProcessorName, IdentityProcessor::new, sourceName);
            }
        }
        if (materializeStore) {
            addStateStoreMapping(topologyConstructor.processorStateStoreMapper, sourceProcessorName, STORE_PREFIX + sourceProcessorName);
            topologyConstructor.stores.add(STORE_PREFIX + sourceProcessorName);
            topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + sourceProcessorName, createMessageStoreSupplier(STORE_PREFIX + sourceProcessorName, true));
        }

        logger.info("Granting access for processor: {} to store: {}", sourceProcessorName, sourceProcessorName);

        return sourceProcessorName;
    }

    public static void addSingleJoinGrouped(final Topology current, TopologyContext topologyContext,
                                              TopologyConstructor topologyConstructor, String fromProcessor, String name,
                                              String withProcessor, boolean optional, boolean materialize, boolean isList) {

        String firstNamePre = name + "-forwardpre";
        String secondNamePre = name + "-reversepre";
        String finalJoin = name + "-joined";

        ProcessorSupplier<String,ReplicationMessage,String,ReplicationMessage> groupProcessors = !isList ? () -> new ManyToOneGroupedProcessor(
                fromProcessor,
                withProcessor,
                optional
        )
                :
                ()-> new ManyToManyGroupedProcessor(
                        fromProcessor,
                        withProcessor,
                        optional
                );

        //Preprocessor - add info whether the resulting message is a reverse-join or not
        current.addProcessor(
                firstNamePre
                , () -> new PreJoinProcessor(false)
                , fromProcessor
        ).addProcessor(
                secondNamePre
                , () -> new PreJoinProcessor(true)
                , withProcessor
        ).addProcessor(
                finalJoin
                , groupProcessors
                , firstNamePre, secondNamePre
        );
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalJoin, STORE_PREFIX + withProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalJoin, STORE_PREFIX + fromProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
        topologyConstructor.stores.add(STORE_PREFIX + withProcessor);
        topologyConstructor.stores.add(STORE_PREFIX + fromProcessor);
        // TODO I think we only need to materialize if materialize=true

        topologyConstructor.stores.add(STORE_PREFIX + name);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, true));
        current.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + name), finalJoin);
    }

// TODO unused topologyContext is suspect
    public static String addGroupedProcessor(final Topology current, TopologyContext topologyContext, TopologyConstructor topologyConstructor, String name, String from,
                                             Function<ReplicationMessage, String> keyExtractor) {

        String mappingStoreName;
        if (!topologyConstructor.stores.contains(STORE_PREFIX + from)) {
            logger.error("Adding grouped with from, no source processor present for: " + from + " created: " + topologyConstructor.stateStoreSupplier.keySet() + " and from: " + from);
        }
        mappingStoreName = from + "_mapping";
        // allow override to avoid clashes
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
        topologyConstructor.stores.add(STORE_PREFIX + name);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + mappingStoreName);
        topologyConstructor.stores.add(STORE_PREFIX + mappingStoreName);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, true));
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + mappingStoreName, createMessageStoreSupplier(STORE_PREFIX + mappingStoreName, true));
        current.addProcessor(name, () -> new GroupedUpdateProcessor(STORE_PREFIX + name, keyExtractor, STORE_PREFIX + mappingStoreName), from);
        return name;
    }


    public static void addPersistentCache(Topology current, TopologyContext topologyContext,
                                          TopologyConstructor topologyConstructor, String name, String fromProcessorName, Duration cacheTime,
                                          int maxSize, boolean inMemory) {
        current.addProcessor(
                name
                , () -> new CacheProcessor(name, cacheTime, maxSize, inMemory)
                , fromProcessorName
        );
        logger.info("Buffer using statestore: {}", STORE_PREFIX + name);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, true));
    }

    public static String addReducer(final Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor,
                                    Stack<String> transformerNames, int currentPipeId, List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove,
                                    Function<ImmutableMessage, ImmutableMessage> initialMessage, boolean materialize, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {

        String parentName = transformerNames.peek();
        String reduceReader = topologyContext.qualifiedName("reduce", transformerNames.size(), currentPipeId);
        transformerNames.push(reduceReader);
        String ifElseName = topologyContext.qualifiedName("ifelse", transformerNames.size(), currentPipeId);
        transformerNames.push(ifElseName);
        int trueBranchPipeId = topologyConstructor.generateNewStreamId();
        int falseBranchPipeId = topologyConstructor.generateNewStreamId();

        String trueBranchName = topologyContext.qualifiedName("addbranch", transformerNames.size(), currentPipeId);
        String falseBranchName = topologyContext.qualifiedName("removeBranch", transformerNames.size(), currentPipeId);

        String reduceName = topologyContext.qualifiedName("reduce", transformerNames.size(), currentPipeId);

        String reduceStoreName = STORE_PREFIX + "accumulator_" + reduceName;
        String inputStoreName = STORE_PREFIX + "reduce_inputstore_"+ reduceName;

        topology.addProcessor(reduceReader, () -> new ReduceReadProcessor(inputStoreName, reduceStoreName, initialMessage, keyExtractor), parentName);
        topology.addProcessor(ifElseName, () -> new IfElseProcessor(msg -> msg.operation() != Operation.DELETE, trueBranchName, Optional.of(falseBranchName)), reduceReader);

        Stack<String> addProcessorStack = new Stack<>();
        addProcessorStack.addAll(transformerNames);

        topology.addProcessor(trueBranchName, IdentityProcessor::new, addProcessorStack.peek());
        addProcessorStack.push(trueBranchName);

        // Copy stack to have an independant stack
        Stack<String> removeProcessorStack = new Stack<>();
        removeProcessorStack.addAll(transformerNames);

        topology.addProcessor(falseBranchName, IdentityProcessor::new, removeProcessorStack.peek());
        removeProcessorStack.push(falseBranchName);

        for (TopologyPipeComponent addBranchComponents : onAdd) {
            addBranchComponents.addToTopology(addProcessorStack, trueBranchPipeId, topology, topologyContext, topologyConstructor);
        }

        String primToSecondaryAddProcessor = topologyContext.qualifiedName("primToSecondaryAdd", transformerNames.size(), currentPipeId);

        topology.addProcessor(primToSecondaryAddProcessor, PrimaryToSecondaryProcessor::new, addProcessorStack.peek());
        addProcessorStack.push(primToSecondaryAddProcessor);

        for (TopologyPipeComponent removePipeComponents : onRemove) {
            removePipeComponents.addToTopology(removeProcessorStack, falseBranchPipeId, topology, topologyContext, topologyConstructor);
        }
        String primToSecondaryRemoveProcessor = topologyContext.qualifiedName("primToSecondaryRemove", transformerNames.size(), currentPipeId);

        topology.addProcessor(primToSecondaryRemoveProcessor, PrimaryToSecondaryProcessor::new, removeProcessorStack.peek());
        removeProcessorStack.push(primToSecondaryRemoveProcessor);

//		topologyConstructor
        topology.addProcessor(materialize ? "_proc" + reduceName : reduceName, () -> new StoreStateProcessor(reduceStoreName), addProcessorStack.peek(), removeProcessorStack.peek());
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, materialize ? "_proc" + reduceName : reduceName, reduceStoreName);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, reduceReader, reduceStoreName);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, reduceReader, inputStoreName);

        if (!topologyConstructor.immutableStoreSupplier.containsKey(reduceStoreName)) {
            topologyConstructor.immutableStoreSupplier.put(reduceStoreName, createImmutableMessageSupplier(reduceStoreName, false));
        }
        if (!topologyConstructor.stateStoreSupplier.containsKey(inputStoreName)) {
            topologyConstructor.stateStoreSupplier.put(inputStoreName, createMessageStoreSupplier(inputStoreName, false));
        }
        if (materialize) {
            addMaterializeStore(topology, topologyContext, topologyConstructor, reduceName, "_proc" + reduceName);
        }
        return reduceName;
    }

    public static void addJoin(final Topology current, TopologyContext topologyContext,
                                   TopologyConstructor topologyConstructor, String fromProcessorName, String withProcessorName, String name,
                                   boolean optional,
                                   boolean multiple,
                                   boolean materialize,
                                   boolean debug) {
        String firstNamePre = name + "-forwardpre";
        String secondNamePre = name + "-reversepre";

        //Preprocessor - add info whether the resulting message is a reverse-join or not
        current.addProcessor(
                firstNamePre
                , () -> new PreJoinProcessor(false)
                , fromProcessorName
        ).addProcessor(
                secondNamePre
                , () -> new PreJoinProcessor(true)
                , withProcessorName
        );

        final ProcessorSupplier<String,ReplicationMessage,String,ReplicationMessage> proc;
        if (multiple) {
            proc = () -> new OneToManyGroupedProcessor(
                    STORE_PREFIX + fromProcessorName,
                    STORE_PREFIX + withProcessorName,
                    optional,
                    debug
            );
        } else {
            proc = () -> new OneToOneProcessor(
                    STORE_PREFIX + fromProcessorName,
                    STORE_PREFIX + withProcessorName,
                    optional,
                    (msg, comsg) -> msg.withParamMessage(comsg.message()), debug);
        }
        String procName = materialize ? "proc_" + name : name;
        current.addProcessor(
                procName
                , proc
                , firstNamePre, secondNamePre
        );
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, procName, STORE_PREFIX + withProcessorName);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, procName, STORE_PREFIX + fromProcessorName);
        if (materialize) {
            topologyConstructor.stores.add(STORE_PREFIX + name);
            topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, true));
            addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
            current.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + name), procName);

        }
    }

    public static StoreBuilder<KeyValueStore<String, ReplicationMessage>> createMessageStoreSupplier(String name, boolean persistent) {
        logger.info("Creating messagestore supplier: {}", name);
        KeyValueBytesStoreSupplier storeSupplier = persistent ? Stores.persistentKeyValueStore(name) : Stores.inMemoryKeyValueStore(name);
        return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), messageSerde);
    }

    public static StoreBuilder<KeyValueStore<String, ImmutableMessage>> createImmutableMessageSupplier(String name, boolean persistent) {
        logger.info("Creating messagestore supplier: {}", name);
        KeyValueBytesStoreSupplier storeSupplier = persistent ? Stores.persistentKeyValueStore(name) : Stores.inMemoryKeyValueStore(name);
        return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), immutableMessageSerde);
    }
}
