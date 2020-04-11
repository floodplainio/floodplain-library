package com.dexels.kafka.streams.remotejoin;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.Filters;
import com.dexels.kafka.streams.processor.generic.GenericProcessorBuilder;
import com.dexels.kafka.streams.remotejoin.ranged.GroupedUpdateProcessor;
import com.dexels.kafka.streams.remotejoin.ranged.ManyToManyGroupedProcessor;
import com.dexels.kafka.streams.remotejoin.ranged.ManyToOneGroupedProcessor;
import com.dexels.kafka.streams.remotejoin.ranged.OneToManyGroupedProcessor;
import com.dexels.kafka.streams.serializer.ImmutableMessageSerde;
import com.dexels.kafka.streams.serializer.ReplicationMessageSerde;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.dexels.kafka.streams.api.CoreOperators.extractKey;
import static com.dexels.kafka.streams.api.CoreOperators.topicName;

public class ReplicationTopologyParser {
    private static final String STORE = "store";
    private static final String CACHE = "cache";

    private static final String DIFFSTORE = "diffstore";
    private static final String JOIN = "join";

    private static final String GROUPEDSTORE = "groupedStore";
    private static final String JOINGROUPED = "joinGrouped";
    private static final String SPLIT = "split";

    public static final String STORE_PREFIX = "STORE_";

    private static final Serde<ReplicationMessage> messageSerde = new ReplicationMessageSerde();
    private static final Serde<ImmutableMessage> immutableMessageSerde = new ImmutableMessageSerde();

    public enum Flatten {FIRST, LAST, NONE}

    ;

    private static final Logger logger = LoggerFactory.getLogger(ReplicationTopologyParser.class);

    private ReplicationTopologyParser() {
        // - no instances
    }


    public static final void addStateStoreMapping(Map<String, List<String>> processorStateStoreMapper, String processor, String stateStore) {
        logger.info("Adding processor: {} with statestore: {}", processor, stateStore);
        System.err.println("Adding processor: " + processor + " with sttestore: " + stateStore);
        List<String> parts = processorStateStoreMapper.get(stateStore);
        if (parts == null) {
            parts = new ArrayList<>();
            processorStateStoreMapper.put(stateStore, parts);
        }
        parts.add(processor);
    }

    private static void addGenericProcessor(Topology current, TopologyContext context,
                                            TopologyConstructor topologyConstructor, GenericProcessorBuilder genericBuilder, Map<String, String> settings, StreamConfiguration config) {

        genericBuilder.build(current, settings, context, config);

    }


    public static void materializeStateStores(TopologyConstructor topologyConstructor, Topology current) {
        for (Entry<String, List<String>> element : topologyConstructor.processorStateStoreMapper.entrySet()) {
            final String key = element.getKey();
            final StoreBuilder<KeyValueStore<String, ReplicationMessage>> supplier = topologyConstructor.stateStoreSupplier.get(key);
            if (supplier == null) {
                final StoreBuilder<KeyValueStore<String, ImmutableMessage>> immutableSupplier = topologyConstructor.immutableStoreSupplier.get(key);
//				supplier = topologyConstructor.immutableStoreSupplier.get(key);
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


    private static void addDiffProcessor(Topology current, TopologyContext context,
                                         TopologyConstructor topologyConstructor, String sourceTopic, String fromProcessor,
                                         Optional<String> destination, final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren,
                                         String diffProcessorNamePrefix) {
        if (sourceTopic != null) {
            String diffStoreTopic = topicName(sourceTopic, context);
            topologyConstructor.addDesiredTopic(diffStoreTopic,Optional.empty());
            current = current.addSource(diffProcessorNamePrefix + "_src", diffStoreTopic)
                    .addProcessor(diffProcessorNamePrefix + "_transform", processorFromChildren.orElse(() -> new IdentityProcessor()), diffProcessorNamePrefix + "_src")
                    .addProcessor(diffProcessorNamePrefix, () -> new DiffProcessor(diffProcessorNamePrefix), diffProcessorNamePrefix + "_transform");
        } else {
            // TODO shouldn't the processorFromChildren be added here too?
            current = current.addProcessor(diffProcessorNamePrefix, () -> new DiffProcessor(diffProcessorNamePrefix), fromProcessor);
        }
        if (destination.isPresent()) {
            addTopicDestination(current, context, topologyConstructor, diffProcessorNamePrefix, destination.get(), diffProcessorNamePrefix, partitionsFromDestination(destination));
        }
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, diffProcessorNamePrefix, diffProcessorNamePrefix);
        logger.info("Granting access for processor: {} to store: {}", diffProcessorNamePrefix, diffProcessorNamePrefix);
        topologyConstructor.stateStoreSupplier.put(diffProcessorNamePrefix, createMessageStoreSupplier(diffProcessorNamePrefix, true));
    }


    public static void addTopicDestination(Topology topology, TopologyContext context, TopologyConstructor topologyConstructor, String processorNamePrefx,
                                           String to, String parentProcessorName, Optional<Integer> partitions) {
        String topicName = topicName(to, context);
        logger.info("Adding sink to: {}", topicName);
        topologyConstructor.addDesiredTopic(topicName,partitions);
        topology.addSink(
                processorNamePrefx + "_sink",
                topicName,
                Serdes.String().serializer(),
                messageSerde.serializer(),
                parentProcessorName
        );
    }

    // unused?
//   public static String addProcessorStore(final Topology currentBuilder, TopologyContext context, TopologyConstructor topologyConstructor,String processorName) {
//	   final String sourceProcessorName = processorName(processorName);
//		addStateStoreMapping(topologyConstructor.processorStateStoreMapper,sourceProcessorName, STORE_PREFIX+sourceProcessorName);
//		topologyConstructor.stores.add(STORE_PREFIX+sourceProcessorName);
//		logger.info("Granting access for processor: {} to store: {}",sourceProcessorName, STORE_PREFIX+sourceProcessorName);
//       topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+sourceProcessorName,createMessageStoreSupplier(STORE_PREFIX+sourceProcessorName,true));
//       return sourceProcessorName;
//   }

    public static String addLazySourceStore(final Topology currentBuilder, TopologyContext context,
                                            TopologyConstructor topologyConstructor, String topicName, Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
//		String storeTopic = topicName(topicName, context);
        // TODO It might be better to fail if the topic does not exist? -> Well depends,
        // if it is external yes, but if it is created by the same instance, then no.
        // No: if the topic is dynamic, it won't exist at first, so better to ensure.
        topologyConstructor.addDesiredTopic(topicName,Optional.empty());
//		final String sourceProcessorName = processorName(sourceTopicName);
//		String sourceName;
        if (!topologyConstructor.sources.containsKey(topicName)) {
//			sourceName = sourceProcessorName + "_src";
            currentBuilder.addSource(topicName, keyDeserializer, valueDeserializer, topicName);
            topologyConstructor.sources.put(topicName, topicName);
            // TODO Optimize. The topology should be valid without adding identityprocessors
        } else {
//    	sourceName = topologyConstructor.sources.get(storeTopic);
//    	currentBuilder.addProcessor(sourceProcessorName,()->new IdentityProcessor(), sourceName);
        }
//		currentBuilder.addProcessor(sourceProcessorName, () -> new IdentityProcessor(), topicName);
        return topicName;
    }

    //    ss
    public static String addMaterializeStore(final Topology currentBuilder, TopologyContext context,
                                             TopologyConstructor topologyConstructor, String name, String parentProcessor) {
        final String sourceProcessorName = name;
        currentBuilder.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + sourceProcessorName), parentProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, sourceProcessorName, STORE_PREFIX + sourceProcessorName);
        topologyConstructor.stores.add(STORE_PREFIX + sourceProcessorName);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + sourceProcessorName, createMessageStoreSupplier(STORE_PREFIX + sourceProcessorName, true));
        return name;
    }

    public static String addSourceStore(final Topology currentBuilder, TopologyContext context, TopologyConstructor topologyConstructor, Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren,
                                        String sourceTopicName,
                                        Optional<String> destination, boolean materializeStore) {
        String storeTopic = topicName(sourceTopicName, context);
        // TODO It might be better to fail if the topic does not exist? -> Well depends, if it is external yes, but if it is created by the same instance, then no.
        final String sourceProcessorName = storeTopic;
        System.err.println("Source proc name: " + sourceProcessorName);

        if (storeTopic != null) {
            String sourceName;
            if (!topologyConstructor.sources.containsKey(storeTopic)) {
                sourceName = sourceProcessorName + "_src";
                currentBuilder.addSource(sourceName, storeTopic);
                topologyConstructor.sources.put(storeTopic, sourceName);
                if (processorFromChildren.isPresent()) {
                    if (materializeStore) {
                        currentBuilder.addProcessor(sourceProcessorName + "_transform", processorFromChildren.get(), sourceName);
                        currentBuilder.addProcessor(sourceProcessorName, () -> new StoreProcessor(STORE_PREFIX + sourceProcessorName), sourceProcessorName + "_transform");
                    } else {
                        currentBuilder.addProcessor(sourceProcessorName, processorFromChildren.get(), sourceName);

                    }
                } else {
                    if (materializeStore) {
                        currentBuilder.addProcessor(sourceProcessorName, () -> new StoreProcessor(STORE_PREFIX + sourceProcessorName), sourceName);
                    } else {
                        currentBuilder.addProcessor(sourceProcessorName, () -> new IdentityProcessor(), sourceName);

                    }
                }

            } else {
                sourceName = topologyConstructor.sources.get(storeTopic);
            }

        }
        if (materializeStore) {
            addStateStoreMapping(topologyConstructor.processorStateStoreMapper, sourceProcessorName, STORE_PREFIX + sourceProcessorName);
            topologyConstructor.stores.add(STORE_PREFIX + sourceProcessorName);
            topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + sourceProcessorName, createMessageStoreSupplier(STORE_PREFIX + sourceProcessorName, true));
        }

        if (destination.isPresent()) {
            addTopicDestination(currentBuilder, context, topologyConstructor, sourceProcessorName, destination.get(), sourceProcessorName, partitionsFromDestination(destination));
        }

        logger.info("Granting access for processor: {} to store: {}", sourceProcessorName, STORE_PREFIX + storeTopic);

        return sourceProcessorName;
    }

//	private static String processorName(String sourceTopicName) {
//        return sourceTopicName.replace(':',  '_').replace('@', '.');
//    }


    // TODO replace, with the new streams api we can extract topic names from messages in a more declarative way
    private static void addSplit(Topology current, TopologyContext topologyContext, String name, Optional<String> from, Optional<String> topic,
                                 Optional<ProcessorSupplier<String, ReplicationMessage>> transformerSupplier, List<XMLElement> destinations, Optional<XMLElement> defaultDestination, Optional<AdminClient> adminClient) throws InterruptedException, ExecutionException {
        String transformProcessor;
        if (from.isPresent()) {
            String sourceProcessor = from.get();
            transformProcessor = name + "_transform";
            current.addProcessor(transformProcessor, transformerSupplier.orElse(() -> new IdentityProcessor()), sourceProcessor);
        } else {
            if (!topic.isPresent()) {
                throw new NullPointerException("In a groupedProcessor you either need a 'from' or a 'topic'");
            }
            transformProcessor = topic.get(); //TODO Huh?
            String topicName = topicName(topic.get(), topologyContext);
            current.addSource(topic.get() + "_src", topicName)
                    .addProcessor(transformProcessor, transformerSupplier.orElse(() -> new IdentityProcessor()), topic.get() + "_src");
        }
        List<Predicate<String, ReplicationMessage>> filterList = new ArrayList<>();
        for (XMLElement destination : destinations) {
            String destinationName = destination.getStringAttribute("name");
            String destinationTopic = topicName(destination.getStringAttribute("topic"), topologyContext);
            Optional<Integer> partitions = Optional.ofNullable(destination.getStringAttribute("partitions")).map(Integer::parseInt);
            String filter = destination.getStringAttribute("filter");
            String keyColumn = destination.getStringAttribute("keyColumn");
            Predicate<String, ReplicationMessage> destinationFilter = Filters.getFilter(Optional.ofNullable(filter)).orElse((key, value) -> true);
            filterList.add(destinationFilter);
            Function<ReplicationMessage, String> keyExtract = extractKey(keyColumn);
            addSplitDestination(current, transformProcessor, destinationName, destinationTopic, keyExtract, destinationFilter, adminClient, partitions);
        }
        if (defaultDestination.isPresent()) {
            String destinationTopic = topicName(defaultDestination.get().getStringAttribute("topic"), topologyContext);
            Optional<Integer> partitions = Optional.ofNullable(defaultDestination.get().getStringAttribute("partitions")).map(Integer::parseInt);

            String keyColumn = defaultDestination.get().getStringAttribute("keyColumn");
            Function<ReplicationMessage, String> keyExtract = extractKey(keyColumn);
            Predicate<String, ReplicationMessage> defaultPredicate = (k, v) -> {
                for (Predicate<String, ReplicationMessage> element : filterList) {
                    if (element.test(k, v)) {
                        return false;
                    }
                }
                return true;
            };
            addSplitDestination(current, transformProcessor, "default", destinationTopic, keyExtract, defaultPredicate, adminClient, partitions);
        }
    }

    private static void addSplitDestination(Topology builder, String parentProcessor, String destinationName, String destinationTopic,
                                            Function<ReplicationMessage, String> keyExtract, Predicate<String, ReplicationMessage> destinationFilter, Optional<AdminClient> adminClient, Optional<Integer> partitions) {
        String destinationProcName = destinationName;

        KafkaUtils.ensureExistsSync(adminClient, destinationTopic, partitions);
        builder.addProcessor(destinationProcName, new DestinationProcessorSupplier(keyExtract, destinationFilter), parentProcessor)
                .addSink(destinationProcName + "_sink", destinationTopic, destinationProcName);
    }

    private static Flatten parseFlatten(String flatten) {
        if (flatten == null) {
            return Flatten.NONE;
        }
        if ("true".equals(flatten) || "first".equals(flatten)) {
            return Flatten.FIRST;
        }
        if ("last".equals(flatten)) {
            return Flatten.LAST;
        }
        return Flatten.NONE;
    }

    private static Optional<Integer> partitionsFromDestination(Optional<String> destination) {
        if (destination.isPresent()) {
            String[] parts = destination.get().split(":");
            if (parts.length > 1) {
                return Optional.of(Integer.parseInt(parts[1]));
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    public static String addSingleJoinGrouped(final Topology current, TopologyContext topologyContext,
                                              TopologyConstructor topologyConstructor, String fromProcessor, String name,
                                              Optional<Predicate<String, ReplicationMessage>> associationBypass,
                                              boolean isList, String withProcessor, boolean optional) {

        String firstNamePre = name + "-forwardpre";
        String secondNamePre = name + "-reversepre";
        String finalJoin = name + "-joined";

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
                , () -> (!isList) ?
                        new ManyToOneGroupedProcessor(
                                fromProcessor,
                                withProcessor,
                                associationBypass,
                                optional
                        )
                        :
                        new ManyToManyGroupedProcessor(
                                fromProcessor,
                                withProcessor,
                                associationBypass,
                                optional
                        )
                , firstNamePre, secondNamePre
        );
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalJoin, STORE_PREFIX + withProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalJoin, STORE_PREFIX + fromProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
        topologyConstructor.stores.add(STORE_PREFIX + withProcessor);
        topologyConstructor.stores.add(STORE_PREFIX + fromProcessor);
        topologyConstructor.stores.add(STORE_PREFIX + name);

        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, true));
        current.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + name), finalJoin);
        return finalJoin;
    }


    public static String addGroupedProcessor(final Topology current, TopologyContext topologyContext, TopologyConstructor topologyConstructor, String name, String from, boolean ignoreOriginalKey,
                                             Function<ReplicationMessage, String> keyExtractor, Optional<ProcessorSupplier<String, ReplicationMessage>> transformerSupplier) {

        String mappingStoreName;
        if (!topologyConstructor.stores.contains(STORE_PREFIX + from)) {
            System.err.println("Adding grouped with from, no source processor present for: " + from + " created: " + topologyConstructor.stateStoreSupplier.keySet() + " and from: " + from);
        }
        mappingStoreName = from + "_mapping";

        String transformProcessor = name + "_transform";
        current.addProcessor(transformProcessor, transformerSupplier.orElse(() -> new IdentityProcessor()), from);
        // allow override to avoid clashes
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
        topologyConstructor.stores.add(STORE_PREFIX + name);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + mappingStoreName);
        topologyConstructor.stores.add(STORE_PREFIX + mappingStoreName);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, true));
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + mappingStoreName, createMessageStoreSupplier(STORE_PREFIX + mappingStoreName, true));
        current.addProcessor(name, () -> new GroupedUpdateProcessor(STORE_PREFIX + name, keyExtractor, STORE_PREFIX + mappingStoreName, ignoreOriginalKey), transformProcessor);
        return name;
    }


    public static void addPersistentCache(Topology current, TopologyContext topologyContext,
                                          TopologyConstructor topologyConstructor, String name, String fromProcessorName, Optional<String> cacheTime,
                                          Optional<String> maxSize, Optional<String> to, Optional<Integer> partitions,
                                          final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren) {
        if (topologyConstructor.stateStoreSupplier.get(fromProcessorName) == null) {
            addSourceStore(current, topologyContext, topologyConstructor, processorFromChildren, fromProcessorName, Optional.empty(), true);
        }

        String nameCache = name + "-cache";

        current.addProcessor(
                nameCache
                , () -> new CacheProcessor(nameCache, cacheTime, maxSize)
                , fromProcessorName
        );
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, nameCache, nameCache);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, name);
        topologyConstructor.stateStoreSupplier.put(name, createMessageStoreSupplier(name, true));
        topologyConstructor.stateStoreSupplier.put(nameCache, createMessageStoreSupplier(nameCache, true));
        current.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + name), nameCache);

        if (to.isPresent()) {
            addTopicDestination(current, topologyContext, topologyConstructor, name, to.get(), nameCache, partitions);
        }
    }

    public static String addReducer(final Topology topology, TopologyContext topologyContext, TopologyConstructor topologyConstructor,
                                    String namespace, Stack<String> transformerNames, int currentPipeId, List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove,
                                    ImmutableMessage initialMessage, boolean materialize, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {

        String parentName = transformerNames.peek();
        String reduceReader = topologyContext.qualifiedName("reduce", transformerNames.size(), currentPipeId);
        transformerNames.push(reduceReader);
        String ifElseName = topologyContext.qualifiedName("ifelse", transformerNames.size(), currentPipeId);
        transformerNames.push(ifElseName);
        int trueBranchPipeId = topologyConstructor.generateNewPipeId();
        int falseBranchPipeId = topologyConstructor.generateNewPipeId();

        String trueBranchName = topologyContext.qualifiedName("addbranch", transformerNames.size(), currentPipeId);
        String falseBranchName = topologyContext.qualifiedName("removeBranch", transformerNames.size(), currentPipeId);

        String reduceName = topologyContext.qualifiedName("reduce", transformerNames.size(), currentPipeId);

        String reduceStoreName = STORE_PREFIX + reduceName;
        String inputStoreName = STORE_PREFIX + parentName + "_reduce_inputstore";

        topology.addProcessor(reduceReader, () -> new ReduceReadProcessor(inputStoreName, reduceStoreName, initialMessage, keyExtractor), parentName);
        topology.addProcessor(ifElseName, () -> new IfElseProcessor(msg -> msg.operation() != Operation.DELETE, trueBranchName, Optional.of(falseBranchName)), reduceReader);

        Stack<String> addProcessorStack = new Stack<>();
        addProcessorStack.addAll(transformerNames);

        topology.addProcessor(trueBranchName, () -> new IdentityProcessor(), addProcessorStack.peek());
        addProcessorStack.push(trueBranchName);


        Stack<String> removeProcessorStack = new Stack<>();
        removeProcessorStack.addAll(transformerNames);
        topology.addProcessor(falseBranchName, () -> new IdentityProcessor(), removeProcessorStack.peek());
        removeProcessorStack.push(falseBranchName);

        for (TopologyPipeComponent addBranchComponents : onAdd) {
            addBranchComponents.addToTopology(addProcessorStack, trueBranchPipeId, topology, topologyContext, topologyConstructor);
        }
        for (TopologyPipeComponent removePipeComponents : onRemove) {
            removePipeComponents.addToTopology(removeProcessorStack, falseBranchPipeId, topology, topologyContext, topologyConstructor);
        }
//		topologyConstructor
        topology.addProcessor(materialize ? "_proc" + reduceName : reduceName, () -> new StoreStateProcessor(reduceName, reduceStoreName, initialMessage, keyExtractor), addProcessorStack.peek(), removeProcessorStack.peek());
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, reduceName, reduceStoreName);
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

    public static Topology addJoin(final Topology current, TopologyContext topologyContext,
                                   TopologyConstructor topologyConstructor, String fromProcessorName, String withProcessorName, String name,
                                   boolean optional,
                                   boolean multiple,
                                   Optional<Predicate<String, ReplicationMessage>> filterPredicate,
                                   boolean materialize) {
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

        @SuppressWarnings("rawtypes") final Processor proc;
        if (multiple) {
            proc = new OneToManyGroupedProcessor(
                    STORE_PREFIX + fromProcessorName,
                    STORE_PREFIX + withProcessorName,
                    optional,
                    filterPredicate,
                    CoreOperators.getListJoinFunctionToParam(false)
            );
//                    ReplicationTopologyParser.createParamListJoinFunction(into.get()));
        } else {
            proc = new OneToOneProcessor(
                    STORE_PREFIX + fromProcessorName,
                    STORE_PREFIX + withProcessorName,
                    optional,
                    filterPredicate,
                    (msg, comsg) -> msg.withParamMessage(comsg.message()));
        }

        String procName = materialize ? "proc_" + name : name;
        current.addProcessor(
                procName
                , () -> proc
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
        return current;
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

    public static StoreBuilder<KeyValueStore<String, Long>> createKeyRowStoreSupplier(String name) {
        logger.info("Creating key/long supplier: {}", name);
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(name);
        return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Long());
    }

    public static String addKeyRowProcessor(Topology current, TopologyContext context,
                                            TopologyConstructor topologyConstructor, String fromProcessor,
                                            String name, boolean materialize) {
//		String from = processorName(fromProcessor);
        current = current.addProcessor(name, () -> new RowNumberProcessor(STORE_PREFIX + name), fromProcessor);

        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX + name);
        logger.info("Granting access for processor: {} to store: {}", name, STORE_PREFIX + name);
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + name, createMessageStoreSupplier(STORE_PREFIX + name, false));
        if (materialize) {
            throw new UnsupportedOperationException("Sorry, didn't implement materialization yet");
        }
        return name;
    }

}
