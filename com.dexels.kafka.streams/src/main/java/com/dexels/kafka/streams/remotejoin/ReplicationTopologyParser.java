package com.dexels.kafka.streams.remotejoin;

import static com.dexels.kafka.streams.api.CoreOperators.extractKey;
import static com.dexels.kafka.streams.api.CoreOperators.getJoinFunction;
import static com.dexels.kafka.streams.api.CoreOperators.getListJoinFunction;
import static com.dexels.kafka.streams.api.CoreOperators.joinFieldList;
import static com.dexels.kafka.streams.api.CoreOperators.topicName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import com.dexels.kafka.streams.serializer.ReplicationMessageSerde;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.transformer.api.MessageTransformer;

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
	public enum Flatten { FIRST,LAST,NONE};
	
	private static final Logger logger = LoggerFactory.getLogger(ReplicationTopologyParser.class);
	
	private ReplicationTopologyParser() {
		// - no instances
	}
	

	public static final void addStateStoreMapping(Map<String,List<String>> processorStateStoreMapper, String processor, String stateStore) {
		logger.info("Adding processor: {} with statestore: {}",processor,stateStore);
		System.err.println("Adding processor: "+processor+" with sttestore: "+stateStore);
		List<String> parts = processorStateStoreMapper.get(stateStore);
		if(parts==null) {
			parts = new ArrayList<>();
			processorStateStoreMapper.put(stateStore, parts);
		}
		parts.add(processor);
	}
	
	public static void topologyFromXML(Topology current,List<XMLElement> xmlList,TopologyContext context, final Map<String,MessageTransformer> initialTransformerRegistry, AdminClient externalAdminClient, Map<String, GenericProcessorBuilder> genericProcessorRegistry, StreamConfiguration streamConfig) throws InterruptedException, ExecutionException {

	    TopologyConstructor topologyConstructor = new TopologyConstructor(Optional.of(initialTransformerRegistry), Optional.of(externalAdminClient));
	    
	    for (XMLElement xml : xmlList) {
	    	Vector<XMLElement> children = xml.getChildren();
	    	
			for (XMLElement xe : children) {

				String sourceTopicName = xe.getStringAttribute("topic");
				switch (xe.getName()) {
				case "processor": {
					String name = xe.getStringAttribute("name");
					GenericProcessorBuilder genericBuilder = genericProcessorRegistry.get(name);
					addGenericProcessor(current,context,topologyConstructor,genericBuilder,xe.attributes(),streamConfig);
//					GenericProcessor processor = genericBuilder.build(xe,Optional.of(this),context.tenant);
//					startedProcessors.add(processor);
					break;
				}
				case STORE:
				{
				  
					final Optional<String> to = Optional.ofNullable(xe.getStringAttribute("to"));
		        	final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren = processorFromChildren(Optional.of(xe), topicName(sourceTopicName, context), topologyConstructor);
					addSourceStore(current, context, topologyConstructor,processorFromChildren, sourceTopicName, to,true);
				}
				break;
				case DIFFSTORE:
				{
					String name = xe.getStringAttribute("name");
					if(name==null) {
						throw new UnsupportedOperationException("A diffstore definition should have a 'name'");
					}
					String from = xe.getStringAttribute("from");
					if(sourceTopicName==null && from == null) {
						throw new UnsupportedOperationException("A diffstore definition should either have a 'topic' or a 'from'");
					}
					Optional<String> toDiffSink = Optional.ofNullable(xe.getStringAttribute("to"));
					
					// TODO add store filter
					final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren = processorFromChildren(Optional.of(xe),sourceTopicName, topologyConstructor);

					String diffProcessorNamePrefix = processorName(name);
					 
					addDiffProcessor(current, context, topologyConstructor, sourceTopicName, from, toDiffSink,
							processorFromChildren, diffProcessorNamePrefix);
				}
					break;
			    case CACHE:
                    addPersistentCacheXML(current, context,topologyConstructor, xe);
                    break;
			  
				case JOIN:
					addJoinXML(current, context,xe ,topologyConstructor);
				    break;
				case GROUPEDSTORE: 
					{
						String from = xe.getStringAttribute("from");
						if(from==null) {
							throw new NullPointerException("'from' required in groupedstore: "+xe);
						}
						String key = xe.getStringAttribute("key");
						if(key==null) {
							throw new NullPointerException("'key' required in groupedstore: "+xe);
						}
						String name = xe.getStringAttribute("name");
						if(name==null) {
							throw new NullPointerException("'name' required in groupedstore: "+xe);
						}
						String topic = xe.getStringAttribute("topic");
						if(topic!=null) {
							throw new NullPointerException("Shouldn't use topics in groupedstore in elemenet: "+xe);
						}
						String sourceTopic = topicName(from, context);
						boolean ignoreOriginalKey = xe.getBooleanAttribute("ignoreOriginalKey", "true", "false", false);
//						Optional<String> filter = Optional.ofNullable(xe.getStringAttribute("filter"));
						addGroupedProcessor(current, context,topologyConstructor, name,from,ignoreOriginalKey,
								CoreOperators.extractKey(key),processorFromChildren(Optional.of(xe),sourceTopic, topologyConstructor));
					}
					break;
				case JOINGROUPED:
					// I *think* that any child-filters will be applied to both streams
					addSingleJoinGroupedXML(current, context, topologyConstructor, xe);
					break;
				case SPLIT:
					{
						String name = xe.getStringAttribute("name");
						Optional<String> topic = Optional.ofNullable(xe.getStringAttribute("topic"));
						Optional<String> from = Optional.ofNullable(xe.getStringAttribute("from"));
						Optional<XMLElement> transformer = Optional.ofNullable(xe.getChildByTagName("transformer"));
						List<XMLElement> destinations = xe.getChildrenByTagName("destination");
						Optional<XMLElement> defaultDestination = Optional.ofNullable(xe.getChildByTagName("default"));
						final String sourceTopic;
						if (from.isPresent()) {
						    sourceTopic = topicName(from.get(), context);
						} else {
						    sourceTopic = "";
						}
						addSplit(current, context, name, from,topic,processorFromChildren(transformer,sourceTopic, topologyConstructor),destinations,defaultDestination, topologyConstructor.adminClient);
					}
					break;
				default:
					break;
				}
			}
	    }
	    materializeStateStores(topologyConstructor, current);
	}


	private static void addGenericProcessor(Topology current, TopologyContext context,
			TopologyConstructor topologyConstructor,GenericProcessorBuilder genericBuilder, Map<String,String> settings, StreamConfiguration config) {

		genericBuilder.build(current,settings, context,config);
		
	}


	public static void materializeStateStores(TopologyConstructor topologyConstructor, Topology current) {
		for (Entry<String,List<String>> element : topologyConstructor.processorStateStoreMapper.entrySet()) {
			final String key = element.getKey();
			final StoreBuilder<KeyValueStore<String, ReplicationMessage>> supplier = topologyConstructor.stateStoreSupplier.get(key);
			if(supplier==null) {
				logger.error("Missing supplier for: {}\nStore mappings: {}",element.getKey(),topologyConstructor.processorStateStoreMapper);
				
			}
			current = current.addStateStore(supplier, element.getValue().toArray(new String[]{}));
			
			logger.info("Added processor: {} with sttstatestores: {} mappings: {}",element.getKey(), element.getValue(),topologyConstructor.processorStateStoreMapper.get(element.getKey()));
	    }
	}


	private static void addDiffProcessor(Topology current, TopologyContext context,
			TopologyConstructor topologyConstructor, String sourceTopic, String fromProcessor,
			Optional<String> destination, final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren,
			String diffProcessorNamePrefix) {
		if(sourceTopic!=null) {
			String diffStoreTopic = topicName(sourceTopic, context);
			KafkaUtils.ensureExistsSync(topologyConstructor.adminClient, diffStoreTopic,Optional.empty());
			current = current.addSource(diffProcessorNamePrefix+"_src", diffStoreTopic)
					.addProcessor(diffProcessorNamePrefix+"_transform",processorFromChildren.orElse(()->new IdentityProcessor()),diffProcessorNamePrefix+"_src")
		    		.addProcessor(diffProcessorNamePrefix,()->new DiffProcessor(diffProcessorNamePrefix),diffProcessorNamePrefix+"_transform");
		} else {
			// TODO shouldn't the processorFromChildren be added here too?
			String diffProcessorFrom = processorName(fromProcessor);
			current = current.addProcessor(diffProcessorNamePrefix,()->new DiffProcessor(diffProcessorNamePrefix),diffProcessorFrom);
		}
		if(destination.isPresent()) {
			addTopicDestination(current, context,topologyConstructor, diffProcessorNamePrefix, destination.get(),diffProcessorNamePrefix,partitionsFromDestination(destination));
		}
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper,diffProcessorNamePrefix, diffProcessorNamePrefix);
		logger.info("Granting access for processor: {} to store: {}",diffProcessorNamePrefix,diffProcessorNamePrefix);
		topologyConstructor.stateStoreSupplier.put(diffProcessorNamePrefix,createMessageStoreSupplier(diffProcessorNamePrefix));
	}


	public static void addTopicDestination(Topology topology, TopologyContext context, TopologyConstructor topologyConstructor, String processorNamePrefx,
			String to, String parentProcessorName, Optional<Integer> partitions) {
		String topicName = topicName(to, context);
		logger.info("Adding sink to: {}",topicName);
    	KafkaUtils.ensureExistsSync(topologyConstructor.adminClient, topicName,partitions);
		topology.addSink(
				processorNamePrefx+"_sink", 
				topicName,
				Serdes.String().serializer(),
				messageSerde.serializer(),
				parentProcessorName
				);
	}
	
	// unused?
   public static String addProcessorStore(final Topology currentBuilder, TopologyContext context, TopologyConstructor topologyConstructor,String processorName) {
	   final String sourceProcessorName = processorName(processorName);
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper,sourceProcessorName, STORE_PREFIX+sourceProcessorName);
		topologyConstructor.stores.add(STORE_PREFIX+sourceProcessorName);
		logger.info("Granting access for processor: {} to store: {}",sourceProcessorName, STORE_PREFIX+sourceProcessorName);
       topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+sourceProcessorName,createMessageStoreSupplier(STORE_PREFIX+sourceProcessorName));
       return sourceProcessorName;
   }

	public static String addLazySourceStore(final Topology currentBuilder, TopologyContext context,
			TopologyConstructor topologyConstructor, String topicName, Deserializer<?> keyDeserializer, Deserializer<?> valueDeserializer) {
//		String storeTopic = topicName(topicName, context);
		// TODO It might be better to fail if the topic does not exist? -> Well depends,
		// if it is external yes, but if it is created by the same instance, then no.
		// No: if the topic is dynamic, it won't exist at first, so better to ensure.
		topologyConstructor.ensureTopicExists(topicName, Optional.empty());
//		final String sourceProcessorName = processorName(sourceTopicName);
//		String sourceName;
		if (!topologyConstructor.sources.containsKey(topicName)) {
//			sourceName = sourceProcessorName + "_src";
			currentBuilder.addSource(topicName,keyDeserializer,valueDeserializer, topicName);
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
		final String sourceProcessorName = processorName(name);
		currentBuilder.addProcessor(name, () -> new StoreProcessor(STORE_PREFIX + sourceProcessorName),parentProcessor);
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, sourceProcessorName,STORE_PREFIX + sourceProcessorName);
		topologyConstructor.stores.add(STORE_PREFIX + sourceProcessorName);
		topologyConstructor.stateStoreSupplier.put(STORE_PREFIX + sourceProcessorName,createMessageStoreSupplier(STORE_PREFIX + sourceProcessorName));
		return name;
	}
   
    public static String addSourceStore(final Topology currentBuilder, TopologyContext context, TopologyConstructor topologyConstructor, Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren, 
            String sourceTopicName,
            Optional<String> destination, boolean materializeStore) {
        String storeTopic = topicName(sourceTopicName, context);
        // TODO It might be better to fail if the topic does not exist? -> Well depends, if it is external yes, but if it is created by the same instance, then no.
        final String sourceProcessorName = processorName(storeTopic);
        System.err.println("Source proc name: "+sourceProcessorName);

        if(storeTopic!=null) {
        	String sourceName;
            if(!topologyConstructor.sources.containsKey(storeTopic)) {
            	sourceName = sourceProcessorName+"_src";
    			currentBuilder.addSource(sourceName, storeTopic);
                topologyConstructor.sources.put(storeTopic,sourceName);
    			if(processorFromChildren.isPresent()) {
    				if(materializeStore) {
    					currentBuilder.addProcessor(sourceProcessorName+"_transform",processorFromChildren.get(), sourceName);
    					currentBuilder.addProcessor(sourceProcessorName,()->new StoreProcessor(STORE_PREFIX+sourceProcessorName), sourceProcessorName+"_transform");
    				} else {
    					currentBuilder.addProcessor(sourceProcessorName,processorFromChildren.get(), sourceName);
    					
    				}
    			} else {
    				if(materializeStore) {
    					currentBuilder.addProcessor(sourceProcessorName,()->new StoreProcessor(STORE_PREFIX+sourceProcessorName), sourceName);
    				} else {
    					currentBuilder.addProcessor(sourceProcessorName,()->new IdentityProcessor(), sourceName);
    					
    				}
    			}
    			
            } else {
            	sourceName = topologyConstructor.sources.get(storeTopic);
            }

        }
        if(materializeStore) {
    		addStateStoreMapping(topologyConstructor.processorStateStoreMapper,sourceProcessorName, STORE_PREFIX+sourceProcessorName);
    		topologyConstructor.stores.add(STORE_PREFIX+sourceProcessorName);
            topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+sourceProcessorName,createMessageStoreSupplier(STORE_PREFIX+sourceProcessorName));
        }

        if(destination.isPresent()) {
			addTopicDestination(currentBuilder, context,topologyConstructor, sourceProcessorName, destination.get(),sourceProcessorName,partitionsFromDestination(destination));
		}
        
		logger.info("Granting access for processor: {} to store: {}",sourceProcessorName, STORE_PREFIX+storeTopic);

        return sourceProcessorName;
    }

	private static String processorName(String sourceTopicName) {
        return sourceTopicName.replace(':',  '_').replace('@', '.');
    }
	
	
	// TODO replace, with the new streams api we can extract topic names from messages in a more declarative way
    private static void addSplit(Topology current, TopologyContext topologyContext, String name, Optional<String> from, Optional<String> topic,
			Optional<ProcessorSupplier<String, ReplicationMessage>> transformerSupplier, List<XMLElement> destinations, Optional<XMLElement> defaultDestination, Optional<AdminClient> adminClient) throws InterruptedException, ExecutionException {
		String transformProcessor;
		if(from.isPresent()) {
			String sourceProcessor = processorName(from.get());
		    transformProcessor = name+"_transform";
		    current.addProcessor(transformProcessor,transformerSupplier.orElse(()->new IdentityProcessor()),sourceProcessor);
		} else {
			if(!topic.isPresent()) {
				throw new NullPointerException("In a groupedProcessor you either need a 'from' or a 'topic'");
			}
		    transformProcessor = topic.get(); //TODO Huh?
			String topicName = topicName(topic.get(),topologyContext);
		    current.addSource(topic.get()+"_src", topicName)
		    	.addProcessor(transformProcessor, transformerSupplier.orElse(()->new IdentityProcessor()), topic.get()+"_src");
		}
		List<Predicate<String,ReplicationMessage>> filterList = new ArrayList<>();
		for (XMLElement destination : destinations) {
			String destinationName = destination.getStringAttribute("name");
			String destinationTopic = topicName(destination.getStringAttribute("topic"), topologyContext);
	        Optional<Integer> partitions = Optional.ofNullable(destination.getStringAttribute("partitions")).map(Integer::parseInt);
			String filter = destination.getStringAttribute("filter");
			String keyColumn = destination.getStringAttribute("keyColumn");
			Predicate<String, ReplicationMessage> destinationFilter = Filters.getFilter(Optional.ofNullable(filter)).orElse((key,value)->true);
			filterList.add(destinationFilter);
			Function<ReplicationMessage,String> keyExtract = extractKey(keyColumn);
			addSplitDestination(current,transformProcessor,destinationName,destinationTopic,keyExtract,destinationFilter,adminClient,partitions);
		}
		if(defaultDestination.isPresent()) {
			String destinationTopic = topicName(defaultDestination.get().getStringAttribute("topic"), topologyContext);
	        Optional<Integer> partitions = Optional.ofNullable(defaultDestination.get().getStringAttribute("partitions")).map(Integer::parseInt);

			String keyColumn = defaultDestination.get().getStringAttribute("keyColumn");
			Function<ReplicationMessage,String> keyExtract = extractKey(keyColumn);
			Predicate<String,ReplicationMessage> defaultPredicate = (k,v)->{
				for (Predicate<String,ReplicationMessage> element : filterList) {
					if(element.test(k, v)) {
						return false;
					}
				}
				return true;
			};
			addSplitDestination(current,transformProcessor,"default",destinationTopic,keyExtract,defaultPredicate,adminClient,partitions);
		}
	}

	private static void addSplitDestination(Topology builder, String parentProcessor, String destinationName, String destinationTopic,
			Function<ReplicationMessage, String> keyExtract, Predicate<String, ReplicationMessage> destinationFilter, Optional<AdminClient> adminClient,Optional<Integer> partitions) {
		String destinationProcName = processorName(destinationName);

		KafkaUtils.ensureExistsSync(adminClient, destinationTopic,partitions);
	    builder.addProcessor(destinationProcName, new DestinationProcessorSupplier(keyExtract, destinationFilter), parentProcessor)
	    		.addSink(destinationProcName+"_sink", destinationTopic, destinationProcName);
	}
	// will propagate null values unchanged
	private static Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren(Optional<XMLElement> xml, String sourceTopicName, TopologyConstructor topologyConstructor) {
		return Optional.of(()->new XmlTransformerProcessor(xml, sourceTopicName, topologyConstructor));
	}
	
	private static Flatten parseFlatten(String flatten) {
		if(flatten==null) {
			return Flatten.NONE;
		}
		if("true".equals(flatten) || "first".equals(flatten)) {
			return Flatten.FIRST;
		}
		if("last".equals(flatten)) {
			return Flatten.LAST;
		}
		return Flatten.NONE;
	}
	
	private static Optional<Integer> partitionsFromDestination(Optional<String> destination) {
		if (destination.isPresent()) {
			String[] parts = destination.get().split(":");
			if(parts.length>1) {
				return Optional.of(Integer.parseInt(parts[1]));
			}
			return Optional.empty();
		} else {
			return Optional.empty();
		}
	}
	
	private static void addSingleJoinGroupedXML(final Topology current, TopologyContext topologyContext, TopologyConstructor topologyConstructor,XMLElement xe) throws InterruptedException, ExecutionException {


		/**
		 * The inner (many-to-one) processor ('teamperson') listens to the inner processor. It will look up the foreign key (using a lambda) in the 'from' processor,
		 * if there is an entry it will join (using the lambda) and propagate.
		 * If optional is true, and there is no entry, the original will be propagated
		 */
		if(xe.getChildren()!=null && !xe.getChildren().isEmpty()) {
			throw new UnsupportedOperationException("Can't have child xml for node: "+JOINGROUPED+" xml is: "+xe.toString());
		}
	    String from = xe.getStringAttribute("from");
        String withSingle = xe.getStringAttribute("with");
        String withList = xe.getStringAttribute("withList");

        Optional<String> into = Optional.ofNullable(xe.getStringAttribute("into"));
        Optional<Integer> intoPartitions = partitionsFromDestination(into);
        String name = xe.getStringAttribute("name");
        Optional<String> columns = Optional.ofNullable(xe.getStringAttribute("columns"));
        String bypass = xe.getStringAttribute("bypass");
        Optional<Predicate<String, ReplicationMessage>> associationBypass = Filters.getFilter(Optional.ofNullable(bypass));

        Flatten flattenEnum = parseFlatten( xe.getStringAttribute("flatten"));
        
        boolean isList = withList !=null;
        String with = withList!=null ? withList : withSingle;
        if(isList && !into.isPresent()) {
        	throw new TopologyDefinitionException("Can not joinGrouped with a list without an 'into'. Spec: "+xe);
        }

        boolean optional = xe.getBooleanAttribute("optional","true","false",false);
        Optional<String> to = Optional.ofNullable(xe.getStringAttribute("to"));
        
        //--
		String finalJoin = addSingleJoinGrouped(current, topologyContext, topologyConstructor, from, into, name,
				columns, associationBypass, flattenEnum, isList, with, optional);


	
		/**
		 * If you supply a 'to' attribute, it will also send
		 */
		if(to.isPresent()) {
			addTopicDestination(current, topologyContext,topologyConstructor, name, to.get(), finalJoin,intoPartitions);
		} else {
		    logger.debug("No sink found in join");
		}
	}


	public static String addSingleJoinGrouped(final Topology current, TopologyContext topologyContext,
			TopologyConstructor topologyConstructor, String from, Optional<String> into, String name,
			Optional<String> columns, Optional<Predicate<String, ReplicationMessage>> associationBypass,
			Flatten flattenEnum, boolean isList, String with, boolean optional) {
		if(from.startsWith("@")) {
		    String fromTopic = topicName(from,topologyContext);
            KafkaUtils.ensureExistsSync(topologyConstructor.adminClient, fromTopic,Optional.empty());
		}
		final String fromProcessor  = processorName(from);
//		if (!topologyConstructor.stores.contains(STORE_PREFIX+fromProcessor)) {
//	    	final Optional<ProcessorSupplier<String, ReplicationMessage>> fromProcessorFromChildren = processorFromChildren(Optional.empty(), topicName(from, topologyContext), topologyConstructor);
//			addSourceStore(current, topologyContext, topologyConstructor, fromProcessorFromChildren,
//                    from, Optional.empty(),true);
//        }
		final String withProcessor  = processorName(with);
    	final Optional<ProcessorSupplier<String, ReplicationMessage>> withProcessorFromChildren = processorFromChildren(Optional.empty(), topicName(with, topologyContext), topologyConstructor);

//        if (!topologyConstructor.stores.contains(STORE_PREFIX+withProcessor) ) {
//        	addSourceStore(current, topologyContext, topologyConstructor, withProcessorFromChildren,
//        			with, Optional.empty(),true);
//        }
//        
        String firstNamePre = name+"-forwardpre";
        String secondNamePre =  name+"-reversepre";
        String finalJoin = name+"-joined";
                
        //Preprocessor - add info whether the resulting message is a reverse-join or not
        current.addProcessor(
                firstNamePre 
                ,()->new PreJoinProcessor(false)
                ,fromProcessor
        ).addProcessor(
                secondNamePre 
                ,()->new PreJoinProcessor(true)
                ,withProcessor
        ).addProcessor(
                finalJoin 
                ,()->(!isList) ? 
					new ManyToOneGroupedProcessor(
				            fromProcessor,
				            withProcessor,
				            associationBypass,
				            into,
				            columns,
				            optional
				            )
					:
					new ManyToManyGroupedProcessor(
				            fromProcessor,
				            withProcessor,
				            associationBypass,
				            into.get(),
				            columns,
				            optional,
				            flattenEnum
				            )
                ,firstNamePre, secondNamePre
            );
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalJoin, STORE_PREFIX+withProcessor);
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, finalJoin, STORE_PREFIX+fromProcessor);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX+name);
        topologyConstructor.stores.add(STORE_PREFIX+withProcessor);
        topologyConstructor.stores.add(STORE_PREFIX+fromProcessor);
        topologyConstructor.stores.add(STORE_PREFIX+name);
        
        topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+name, createMessageStoreSupplier(STORE_PREFIX+name));
	    current.addProcessor(name,()->new StoreProcessor(STORE_PREFIX+name),finalJoin);
		return finalJoin;
	}
	

	public static String addGroupedProcessor(final Topology current, TopologyContext topologyContext, TopologyConstructor topologyConstructor, String name, String from, boolean ignoreOriginalKey, 
			Function<ReplicationMessage,String>  keyExtractor, Optional<ProcessorSupplier<String,ReplicationMessage>> transformerSupplier) {

		String sourceProcessorName;
		String mappingStoreName;
	    sourceProcessorName = processorName(from);
	    if(!topologyConstructor.stores.contains(STORE_PREFIX+sourceProcessorName)) {
// 			if (topologyConstructor.stateStoreSupplier.get(sourceProcessorName) == null) {
	    	System.err.println("Adding grouped with from, no source processor present for: "+sourceProcessorName+" created: "+topologyConstructor.stateStoreSupplier.keySet()+" and from: "+from);
//		    	final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren = processorFromChildren(Optional.empty(), topicName(from.get(), topologyContext), topologyConstructor);
	    	// TODO removed, is that ok?
	    	
//	    	public static String addLazyStore(final Topology currentBuilder, TopologyContext context,
//	    			TopologyConstructor topologyConstructor, String name, String parentProcessor) {
	    	
//	    	addLazySourceStore(current, topologyContext, topologyConstructor, 
//                   from);
        }
		
		mappingStoreName = sourceProcessorName + "_mapping";

		String transformProcessor = name+"_transform";
		current.addProcessor(transformProcessor,transformerSupplier.orElse(()->new IdentityProcessor()),sourceProcessorName);
		// allow override to avoid clashes
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX+name);
		topologyConstructor.stores.add(STORE_PREFIX+name);
//		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, sourceProcessorName);
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX+mappingStoreName);
		topologyConstructor.stores.add(STORE_PREFIX+mappingStoreName);
		
		topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+name, createMessageStoreSupplier(STORE_PREFIX+name));
		topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+mappingStoreName, createMessageStoreSupplier(STORE_PREFIX+mappingStoreName));

		current.addProcessor(name,()->new GroupedUpdateProcessor(STORE_PREFIX+name,keyExtractor,STORE_PREFIX+mappingStoreName,ignoreOriginalKey),transformProcessor);
		return name;
	}	
	



    private static void addPersistentCacheXML(final Topology builderr, TopologyContext topologyContext, TopologyConstructor topologyConstructor, XMLElement xe) {
        
    	Topology current = builderr;
        String name = xe.getStringAttribute("name");
        String from = xe.getStringAttribute("from");
        Optional<String> cacheTime =  Optional.ofNullable(xe.getStringAttribute("cacheTimeSec"));
        Optional<String> maxSize =  Optional.ofNullable(xe.getStringAttribute("maxSize"));
        Optional<String> to = Optional.ofNullable(xe.getStringAttribute("to"));
		Optional<Integer> partitions =  Optional.ofNullable(xe.getStringAttribute("partitions")).map(Integer::parseInt);

    	// is always empty: TODO
        final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren = processorFromChildren(Optional.empty(), topicName(from, topologyContext),topologyConstructor);

        addPersistentCache(current, topologyContext, topologyConstructor, name, from, cacheTime, maxSize, to,
				partitions, processorFromChildren);
    }


	public static void addPersistentCache(Topology current, TopologyContext topologyContext,
			TopologyConstructor topologyConstructor, String name, String from, Optional<String> cacheTime,
			Optional<String> maxSize, Optional<String> to, Optional<Integer> partitions,
			final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren) {
		final String fromProcessorName = processorName(from);
    	if (topologyConstructor.stateStoreSupplier.get(fromProcessorName) == null) {
        	addSourceStore(current, topologyContext, topologyConstructor, processorFromChildren, from, Optional.empty(),true);
        }
        
        String nameCache = name+"-cache";
        
        current.addProcessor(
                nameCache
                ,()->new CacheProcessor(nameCache, cacheTime, maxSize)
                ,fromProcessorName
        );
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, nameCache, nameCache);
        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, name);
        topologyConstructor.stateStoreSupplier.put(name,createMessageStoreSupplier(name));
        topologyConstructor.stateStoreSupplier.put(nameCache,createMessageStoreSupplier(nameCache));
        current.addProcessor(name,()->new StoreProcessor(STORE_PREFIX+name),nameCache);
        
        if(to.isPresent()) {
        	addTopicDestination(current, topologyContext,topologyConstructor, name, to.get(), nameCache,partitions);
        } 
	}

	private static void addJoinXML(final Topology builderr, TopologyContext topologyContext, XMLElement xe, TopologyConstructor topologyConstructor) {

      List<XMLElement> children = xe.getChildren();
      if(children !=null && !children.isEmpty()) {
    	  throw new UnsupportedOperationException("Sorry, removed sub-transformers from joins. Please transform the relevant source. Offending xml: "+xe);
      }
        String from = xe.getStringAttribute("from");
        String withSingle = xe.getStringAttribute("with");
        String withList = xe.getStringAttribute("withList");
        boolean isList = withList !=null;
        String with = isList ? withList : withSingle;
      
        Optional<String> into = Optional.ofNullable(xe.getStringAttribute("into"));
        String name = xe.getStringAttribute("name");
        Optional<String> columns =  Optional.ofNullable(xe.getStringAttribute("columns"));
        boolean optional = xe.getBooleanAttribute("optional","true","false",false);
        Optional<String> filter = Optional.ofNullable(xe.getStringAttribute("filter"));
        Optional<String> to = Optional.ofNullable(xe.getStringAttribute("to"));
        Optional<String> keyField = Optional.ofNullable(xe.getStringAttribute("keyField"));
        Optional<String> valueField = Optional.ofNullable(xe.getStringAttribute("valueField"));
        BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> listJoinFunction = createJoinFunction(isList, into, name, columns, keyField, valueField);
        final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction = getJoinFunction(into,columns);

        Optional<Predicate<String, ReplicationMessage>> filterPredicate = Filters.getFilter(filter);
        Topology current = addJoin(builderr, topologyContext, topologyConstructor, from, isList, with, name, optional,
				listJoinFunction, joinFunction, filterPredicate);

        
//        Add processor here?

 

		/**
		 * If you supply a 'to' attribute, it will also send
		 */
		if(to.isPresent()) {
			addTopicDestination(current, topologyContext, topologyConstructor, name, to.get(), name,partitionsFromDestination(to));
		} else {
		    logger.debug("No sink found in join");
		}
		
	}


	public static Topology addJoin(final Topology current, TopologyContext topologyContext,
			TopologyConstructor topologyConstructor, String from, boolean isList, String with, String name,
			boolean optional,
			BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> listJoinFunction,
			final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction,
			Optional<Predicate<String, ReplicationMessage>> filterPredicate) {
		KafkaUtils.ensureExistsSync(topologyConstructor.adminClient, topicName(from,topologyContext),Optional.empty());
        final String fromProcessorName = processorName(from);
		final String withProcessorName  = processorName(with);
		String firstNamePre = name+"-forwardpre";
		String secondNamePre =  name+"-reversepre";
		String finalJoin = name+"-joined";

//		if (!topologyConstructor.stores.contains(STORE_PREFIX+fromProcessorName)) {
//	    	final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren = processorFromChildren(Optional.empty(), topicName(from, topologyContext),topologyConstructor);
//			addSourceStore(current, topologyContext, topologyConstructor,  processorFromChildren,from, Optional.empty(),true);
//		}
//		if (!topologyConstructor.stores.contains(STORE_PREFIX+withProcessorName)) {
//	    	final Optional<ProcessorSupplier<String, ReplicationMessage>> processorFromChildren = processorFromChildren(Optional.empty(), topicName(with, topologyContext), topologyConstructor);
//        	addSourceStore(current, topologyContext, topologyConstructor,processorFromChildren,
//        			 with, Optional.empty(),true);
//        }
        
        
        //Preprocessor - add info whether the resulting message is a reverse-join or not
        current.addProcessor(
                firstNamePre 
                ,()->new PreJoinProcessor(false)
                ,fromProcessorName
        ).addProcessor(
                secondNamePre 
                ,()->new PreJoinProcessor(true)
                ,withProcessorName
        );
        
        @SuppressWarnings("rawtypes")
        final Processor proc;
        if (isList) {
            proc = new OneToManyGroupedProcessor(
                     STORE_PREFIX+fromProcessorName,
                     STORE_PREFIX+withProcessorName,
                     optional,
                     filterPredicate,
                     listJoinFunction);
        } else {
			proc = new OneToOneProcessor(
					STORE_PREFIX+fromProcessorName,
					STORE_PREFIX+withProcessorName,
                    optional,
                    filterPredicate,
                    joinFunction);
        }

        // Create single processor to process changes from the processors
        // 

        current.addProcessor(
                name 
                ,()->proc
                ,firstNamePre,secondNamePre
            );

        String lastJoinId = finalJoin;
        
        // TODO fix stores if needed
//        addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX+lastJoinId);
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX+withProcessorName);
		addStateStoreMapping(topologyConstructor.processorStateStoreMapper, name, STORE_PREFIX+fromProcessorName);

//		topologyConstructor.stores.add(STORE_PREFIX+name);
//		topologyConstructor.stateStoreSupplier.put(STORE_PREFIX+name,createMessageStoreSupplier(STORE_PREFIX+name));
//        current.addProcessor(name,()->new StoreProcessor(STORE_PREFIX+name),lastJoinId);
		return current;
	}

	public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> createListJoinFunction(
			String into, String name, Optional<String> columns) {
		return createJoinFunction(true, Optional.of(into), name, columns, Optional.empty(), Optional.empty());
	}
	
	public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> createParamListJoinFunction(String into) {
		return (msg,list)->{
			ReplicationMessage combinedMessage = msg.withParamMessage(ImmutableFactory.empty().withSubMessages(into, list.stream().map(ReplicationMessage::message).collect(Collectors.toList())));
			return combinedMessage;
		};
	}
	
	public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> createJoinFunction(
			boolean isList, Optional<String> into, String name, Optional<String> columns, Optional<String> keyField,
			Optional<String> valueField) {
		final BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> listJoinFunction;
        if (isList && !keyField.isPresent()) {
        	if(!into.isPresent()) {
        		throw new TopologyDefinitionException("Missing into in join definition: "+name+" into is required when joining with a list");
        	} else {
				listJoinFunction = getListJoinFunction(into.get(),false, columns);
        	}
        } else {
            listJoinFunction =  (m1,m2)-> joinFieldList(m1, m2,keyField.get(),valueField.get(),Collections.emptyList(),Optional.empty());
        }
		return listJoinFunction;
	}
	public static StoreBuilder<KeyValueStore<String, ReplicationMessage>> createMessageStoreSupplier(String name) {
		logger.info("Creating messagestore supplier: {}",name);
		KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(name);
		return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), messageSerde);

	}

}
