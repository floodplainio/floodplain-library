package io.floodplain.streams.base;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.transformer.api.MessageTransformer;
import io.floodplain.streams.api.CoreOperators;
import io.floodplain.streams.api.StreamConfiguration;
import io.floodplain.streams.api.StreamTopologyException;
import io.floodplain.streams.processor.generic.GenericProcessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class StreamInstance {
    private final Map<String, KafkaStreams> streams = new HashMap<>();
    private final Map<String, TopologyDescription> topologyDescriptions = new HashMap<>();
    private final Set<ConnectSink> sinks = new HashSet<>();

    private final Set<KafkaStreams> startedStreams = new HashSet<>();

    private final Set<GenericProcessor> startedProcessors = new HashSet<>();

    private static final Logger logger = LoggerFactory.getLogger(StreamInstance.class);

    private final StreamConfiguration configuration;
    private final Map<String, MessageTransformer> transformerRegistry;
    private final String instanceName;
    private static final int kafkaThreadCount;
    private static final int commitInterval;
    private static final long maxBytesBuffering;
    private final AdminClient adminClient;
    private String generation = null;

    static {
        String threadCount = System.getenv("KAFKA_STREAMS_THREADCOUNT");
        if (threadCount == null) {
            kafkaThreadCount = 1;
        } else {
            kafkaThreadCount = Integer.parseInt(threadCount);
        }

        String commitInt = System.getenv("KAFKA_COMMIT_INTERVAL");
        if (commitInt == null) {
            commitInterval = 1000;
        } else {
            commitInterval = Integer.parseInt(commitInt);
        }
        String maxBytesBuffer = System.getenv("KAFKA_CACHE_MAX_BYTES_BUFFERING");
        if (maxBytesBuffer == null) {
            maxBytesBuffering = 10 * 1024 * 1024L;
        } else {
            maxBytesBuffering = Long.parseLong(maxBytesBuffer);
        }

    }

    public StreamInstance(String instanceName, StreamConfiguration configuration, AdminClient adminClient, Map<String, MessageTransformer> transformerRegistry) {
        this.instanceName = instanceName;
        this.configuration = configuration;
        this.transformerRegistry = Collections.unmodifiableMap(transformerRegistry);
        this.adminClient = adminClient;
        Filters.registerPredicate("clublogo", (id, params, message) -> "CLUBLOGO".equals(message.columnValue("objecttype")) && message.columnValue("data") != null);
        Filters.registerPredicate("photo", (id, params, message) -> "PHOTO".equals(message.columnValue("objecttype")) && message.columnValue("data") != null);
        Filters.registerPredicate("facility", (id, params, message) -> "FACILITY".equals(message.columnValue("facilitytype")));
        Filters.registerPredicate("subfacility", (id, params, message) -> !"FACILITY".equals(message.columnValue("facilitytype")));

        Filters.registerPredicate("subfacility_facility", (id, params, message) -> "FACILITY".equals(message.columnValue("subfacilityid")));
        Filters.registerPredicate("subfacility_not_facility", (id, params, message) -> !"FACILITY".equals(message.columnValue("subfacilityid")));
        Filters.registerPredicate("valid_calendar_activityid", (id, params, message) -> {
            if (message.columnValue("activityid") == null) {
                logger.warn("Null activityid! key: {}. Message: {}", message.queueKey(), message);
                return false;
            }
            return ((Integer) message.columnValue("activityid")) >= 20;
        });
        Filters.registerPredicate("notnull", (id, params, message) -> (message.columnValue(params.get(0))) != null);
        Filters.registerPredicate("null", (id, params, message) -> (message.columnValue(params.get(0))) == null);
        Filters.registerPredicate("greaterthan", (id, params, message) -> ((Integer) message.columnValue(params.get(0))) > Integer.parseInt(params.get(1)));
        Filters.registerPredicate("lessthan", (id, params, message) -> ((Integer) message.columnValue(params.get(0))) < Integer.parseInt(params.get(1)));
        Filters.registerPredicate("equalToString", (id, params, message) -> params.get(1).equals(message.columnValue(params.get(0))));
        Filters.registerPredicate("notEqualToString", (id, params, message) -> !params.get(1).equals(message.columnValue(params.get(0))));
        Filters.registerPredicate("notEqualToString", (id, params, message) -> !params.get(1).equals(message.columnValue(params.get(0))));
        Filters.registerPredicate("validZip", this::isValidZipCode);
    }

    public String instanceName() {
        return this.instanceName;
    }

    public Map<String, Boolean> streamStates() {
        Map<String, Boolean> result = new HashMap<>();
        streams.entrySet().stream().forEach(e -> {
            result.put(e.getKey(), startedStreams.contains(e.getValue()));
        });
        return result;
    }

    public Map<String, TopologyDescription> getTopologyDescriptions() {
        return Collections.unmodifiableMap(this.topologyDescriptions);
    }

    private boolean isValidZipCode(String id, List<String> params, ReplicationMessage msg) {
        String zipColumn = params.get(0);
        Object val = msg.columnValue(zipColumn);
        if (val == null) {
            return false;
        }
        if (!(val instanceof String)) {
            return false;
        }
        String zipString = (String) val;
        if (zipString.length() == 6 || zipString.length() == 7) {
            String numbers = zipString.substring(0, 4);
            return numbers.chars().allMatch(Character::isDigit);
        } else {
            return false;
        }
    }

    public void start() {
        for (Entry<String, KafkaStreams> entry : streams.entrySet()) {
            try {
                logger.info("Starting stream entry: {}", entry.getKey());
                final KafkaStreams value = entry.getValue();
                value.setUncaughtExceptionHandler((thread, exception) -> logger.error("Uncaught exception from stream instance: ", exception));
                value.start();
                boolean isRunning = value.state().isRunning();
                String stateName = value.state().name();
                logger.info("State: {} is running? {}", stateName, isRunning);
                startedStreams.add(value);
                final Collection<StreamsMetadata> allMetadata = value.allMetadata();
            } catch (IllegalStateException e) {
                logger.error("IllegalStateException on starting stream {}!", instanceName, e);
            } catch (StreamTopologyException t) {
                logger.error("Exception on starting stream {}!", instanceName, t);
            }

        }
    }

//	private void registerSinkTransformer(XMLElement x, TopologyContext context, String sinkName) {
//		for(XMLElement e : x.getChildren()) {
//			String sinkInstance = e.getStringAttribute("instance");
//			if (sinkInstance == null) {
//			    sinkInstance = instanceName;
//			}
//			TopologyContext currentContext = sinkInstance == null? context: context.withInstance(sinkInstance);
//			String topicName = CoreOperators.topicName( e.getStringAttribute("topic"),currentContext);
//			Optional<MessageTransformer> transformer = transformersFromChildren(Optional.of(e),transformerRegistry,topicName);
//			if(transformer.isPresent()) {
//				SinkTransformerRegistry.registerTransformerForSink(sinkName, currentContext,topicName, transformer.get());
//			}
//		}
//	}
//	
//	public TopicPublisher createPublisher() {
//		return KafkaClientFactory.createPublisher(this.configuration.kafkaHosts(),1,this.configuration.kafkaReplicationFactor());
//	}
//
//	public TopicSubscriber createSubscriber() {
//		Map<String,String> conf = new HashMap<>();
//		conf.put("wait", ""+this.configuration.kafkaSubscribeMaxWait());
//		conf.put("max", ""+this.configuration.kafkaSubscribeMaxSize());
//		return KafkaClientFactory.createSubscriber(this.configuration.kafkaHosts(), conf);
//	}
//	
//	private void addSinks(String sinkType, XMLElement x, String generation, String brokers, Optional<String> tenant, String deployment, File storageFolder, int index) throws IOException, InterruptedException, ExecutionException {
//		boolean useDirectSinks = System.getenv("DIRECT_SINK")!=null && "true".equals(System.getenv("DIRECT_SINK"));
//	   createSink(determineSinkType(sinkType,useDirectSinks),x, adminClient, generation, tenant, deployment, storageFolder);
//    }

//	private SinkType determineSinkType(String sinkTypeName, boolean useDirectSinks) {
//		switch(sinkTypeName) {
//		case "mongodb":
//			return useDirectSinks? SinkType.MONGODBDIRECT : SinkType.MONGODB;
//		case "elasticsearch":
//			return useDirectSinks? SinkType.ELASTICSEARCHDIRECT : SinkType.ELASTICSEARCH;
//		case "neo4j":
//			return SinkType.NEO4J;
//		default:
//			throw new IllegalArgumentException("Unknown sink type: "+sinkTypeName);
//		}
//	}

//	private void createSink(SinkType type, XMLElement x, AdminClient adminClient, String generation, Optional<String> tenant, String deployment, File storageFolder) throws IOException, InterruptedException, ExecutionException {
//		TopologyContext topologyContext = new TopologyContext(tenant, deployment, this.instanceName, generation);
//		String name = x.getStringAttribute("name");
//		// TODO deprecate this?
//		if (name == null) {
//	    	logger.warn("Sink without name found: {}",x.toString());
//	        Map<String, ConnectConfiguration> sinkconfigs = configuration.connectors();
//	        for (String key  : sinkconfigs.keySet()) {
//	            addSinkConfig(type,x, topologyContext, key, sinkconfigs.get(key),adminClient, storageFolder);
//	        }
//	    } else {
//	        Optional<ConnectConfiguration> sinkConfig = configuration.connector(name);
//	        if(sinkConfig.isPresent()) {
//	            addSinkConfig(type,x,topologyContext, name, sinkConfig.get(),adminClient, storageFolder);
//	        } else {
//	            logger.warn("Unable to find {} sink {}!",type, name);
//	        }
//	    }
//	}

//    private void addSinkConfig(SinkType type, XMLElement x, TopologyContext topologyContext, String name, ConnectConfiguration sinkConfig, AdminClient adminClient, File storageFolder) throws IOException, InterruptedException, ExecutionException {
//    	final String sinkTenant = sinkConfig.settings().get("tenant");
//		if (sinkTenant != null && topologyContext.tenant.isPresent()) {
//            if (!sinkTenant.equalsIgnoreCase(topologyContext.tenant.get())) {
//                logger.info("Skipping sink {} for {} due to tenant mismatch: {} {}", name, instanceName, topologyContext.tenant, sinkTenant);
//                return;
//            }
//        }
//        registerSinkTransformer(x,topologyContext,name);
//        final ConnectSink connect;
//        switch(type) {
//        	case MONGODB:
//        		connect =  new RunKafkaConnect(Optional.of(x),configuration,topologyContext,adminClient, name, storageFolder);
//        		break;
//        	case ELASTICSEARCH:
//        		connect =  new RunKafkaSinkElastic(x,configuration,topologyContext, adminClient, name, storageFolder);
//        		break;
//        	default:
//        		throw new StreamTopologyException("Unsupported sink type: "+type.toString());
//        }
//        sinks.add(connect);
//        connect.start();
//    }

    public StreamConfiguration getConfig() {
        return configuration;
    }

    public static Properties createProperties(String applicationId, String brokers, String storagePath) {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        logger.info("Creating application with name: {}", applicationId);
        logger.info("Creating application id: {}", applicationId);
        logger.info("Starting instance in storagePath: {}", storagePath);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamOperators.replicationSerde.getClass());

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        streamsConfiguration.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 40000);
        streamsConfiguration.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000);
        streamsConfiguration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 7200000);
        streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        streamsConfiguration.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, storagePath);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, kafkaThreadCount);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.RETRIES_CONFIG, 50);
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, CoreOperators.topicReplicationCount());
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        // 24h for now
        streamsConfiguration.put("retention.ms", 3600 * 24 * 1000);

        // 10 weeks for now
        streamsConfiguration.put("message.timestamp.difference.max.ms", 604800000 * 10);
        streamsConfiguration.put("log.message.timestamp.difference.max.ms", 604800000 * 11);

//	    StreamsConfig.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, maxBytesBuffering);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitInterval);

        streamsConfiguration.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 7900000);
        streamsConfiguration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 7900000);
        streamsConfiguration.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigurationSetter.class);
        return streamsConfiguration;
    }


    public Collection<KafkaStreams> getStreams() {
        return streams.values();
    }

    public void shutdown() {
        for (ConnectSink sink : sinks) {
            logger.info("Closing sink instance: {}", sink);
            try {
                sink.shutdown();
            } catch (Throwable e) {
                logger.error("Error: ", e);
            }
            logger.info("Done closing sink instance: {}", sink);
        }

        for (KafkaStreams kafkaStream : streams.values()) {
            logger.info("Closing stream instance: {}", kafkaStream);
            kafkaStream.close();
            logger.info("Done closing stream instance: {}", kafkaStream);
            startedStreams.remove(kafkaStream);
        }
        for (GenericProcessor processor : startedProcessors) {
            processor.stop();
        }
        sinks.clear();
        streams.clear();
        topologyDescriptions.clear();
        logger.info("Steams shutdown complete");
    }

}
