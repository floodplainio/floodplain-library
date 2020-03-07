package com.dexels.elasticsearch.sink;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.api.sink.ConnectConfiguration;
import com.dexels.kafka.streams.base.ConnectSink;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.*;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RunKafkaSinkElastic implements ConnectSink {
	private static final Logger logger = LoggerFactory.getLogger(RunKafkaSinkElastic.class);

	private Worker worker;
	private StandaloneHerder herder;
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private final CountDownLatch stopLatch = new CountDownLatch(1);

	OffsetBackingStore offsetBackingStore;
   private Properties[] connectorConfigs;
	private Properties workerProperties;
	private Properties sinkProperties;
	
	public RunKafkaSinkElastic(XMLElement x, StreamConfiguration config, TopologyContext topologyContext, AdminClient adminClient, String sinkName, File storageFolder) throws IOException  {
		Optional<ConnectConfiguration> sinkConfig = config.connector(sinkName);
		if(!sinkConfig.isPresent()) {
			throw new IllegalArgumentException("No sink found: "+sinkName);
		}

		String generationalGroup = CoreOperators.generationalGroup("elasticconnect-"+sinkName, topologyContext);
        final Map<String, Map<String, String>> sinkSettings = sinkTopic(x.getChildren(),topologyContext);

		Map<String,String> topicMapping = sinkSettings.entrySet().stream().collect(Collectors.toMap(e->e.getKey(), e->Optional.ofNullable(e.getValue().get("index")).orElse(""))); 
		Map<String,String> typeMapping = sinkSettings.entrySet().stream().collect(Collectors.toMap(e->e.getKey(), e->Optional.ofNullable(e.getValue().get("type")).orElse(""))); 
		Map<String,String> settings = sinkConfig.get().settings();
		workerProperties = new Properties();
		sinkProperties = new Properties();
		sinkProperties.load(RunKafkaSinkElastic.class.getResourceAsStream("sink.properties"));
		workerProperties.load(RunKafkaSinkElastic.class.getResourceAsStream("worker.properties"));
		sinkProperties.put("name", generationalGroup);
		sinkProperties.putAll(settings);
		KafkaUtils.ensureExistSync(Optional.of(adminClient), topicMapping.keySet(),CoreOperators.topicPartitionCount(),CoreOperators.topicReplicationCount());
		final String joinedTopics = String.join(",",topicMapping.entrySet().stream().map(e->e.getKey()).collect(Collectors.toList()));
		sinkProperties.put(ElasticSinkConnector.TOPICS,joinedTopics);
		final String joinedIndexes = String.join(",",topicMapping.entrySet().stream().map(e->e.getValue()).collect(Collectors.toList()));
		sinkProperties.put(ElasticSinkConnector.INDEXES,joinedIndexes);
		final String joinedTypes = String.join(",",typeMapping.entrySet().stream().map(e->e.getValue()).collect(Collectors.toList()));
		sinkProperties.put(ElasticSinkConnector.TYPES,joinedTypes);

		workerProperties.put(ElasticSinkConnector.TOPICS,joinedTopics);
		workerProperties.put(ElasticSinkConnector.INDEXES,joinedIndexes);
		workerProperties.put(ElasticSinkConnector.TYPES,joinedTypes);

		sinkProperties.put("generation",topologyContext.generation);
		sinkProperties.put("instanceName",topologyContext.instance);
		sinkProperties.put("sinkName",sinkName);
		final Optional<String> maybeTenant = topologyContext.tenant;
		if(maybeTenant.isPresent()) {
			sinkProperties.put("tenant",maybeTenant.get());
		}
		sinkProperties.put("deployment",topologyContext.deployment);
		
		workerProperties.put("bootstrap.servers", String.join(",", config.kafkaHosts()));
		workerProperties.put("group.id", generationalGroup);
		workerProperties.put("compression.type", "lz4");
		workerProperties.put(ElasticSinkConnector.BULK_SIZE, "100");
		
		File output = new File(storageFolder, "elasticoffset-"+generationalGroup);
		workerProperties.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, output.getAbsolutePath());
		logger.info("Sink: {}", sinkProperties);
		logger.info("Worker: {}", workerProperties);
		setupConfig();
	}
	
	

	private final Map<String,Map<String,String>> sinkTopic(List<XMLElement> topicMapping, TopologyContext topologyContext) {
		Map<String,Map<String,String>> result = new HashMap<>();
		List<String> collections = new ArrayList<>();
		for(XMLElement e :topicMapping) {
			String index = e.getStringAttribute("index");
			Map<String,String> attributes = e.attributes();
			
			collections.add(index);
			String topicName = CoreOperators.topicName(e.getStringAttribute("topic"),topologyContext);
			logger.info("Connecting topic: {} to elasticsearch index: {}",topicName,index);
			result.put(topicName, attributes);
		}
		System.err.println("TopicMapping total: "+result);
		return Collections.unmodifiableMap(result);
	}
	
	
	private void setupConfig()  {
	    Properties[] connectorConfigProperties = new Properties[] { this.sinkProperties };
		Time time = Time.SYSTEM;
        logger.info("Kafka Connect standalone worker initializing ...");
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();
		Map<String, String> propsToStringMap = Utils.propsToStringMap(workerProperties);
		System.err.println("worker: "+workerProperties);
		WorkerConfig config = new StandaloneConfig(propsToStringMap);
		String clusterId = ConnectUtils.lookupKafkaClusterId(config);
		logger.info("Using clusterid: {}",clusterId);
		offsetBackingStore = new FileOffsetBackingStore();
		offsetBackingStore.configure(config);

        Plugins plugins = new Plugins(propsToStringMap);
        plugins.compareAndSwapWithDelegatingLoader();
			
		worker = new Worker(clusterId, time, plugins, config, offsetBackingStore);
		herder = new StandaloneHerder(worker,clusterId);
		this.connectorConfigs = connectorConfigProperties; 

	}

	public void start() {
			logger.info("Kafka ConnectEmbedded starting. # of connectorConfigs: {}",connectorConfigs.length);
			logger.info("Configs: {}",Utils.propsToStringMap(connectorConfigs[0]));
			offsetBackingStore.start();
			worker.start();
			for (Properties connectorConfig : connectorConfigs) {
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
				    if (error != null) {
				        logger.error("Failed to create job for {}", connectorConfig);
				        logger.error("Error: ",error);
				    } else {
				        logger.info("Created connector {}", info.result().name());
				    }
				});
				
				String name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
                final Map<String, String> propsToStringMap = Utils.propsToStringMap(connectorConfig);
                logger.info("Props: {}",propsToStringMap);
				herder.putConnectorConfig(name, propsToStringMap, true, cb);
		    }   
	}

	@Override
	public void shutdown() {
		try {
			boolean wasShuttingDown = shutdown.getAndSet(true);
			if (!wasShuttingDown) {
				logger.info("Kafka ConnectEmbedded stopping");
				
				worker.stop();
				offsetBackingStore.stop();
				herder.stop();
				logger.info("Kafka ConnectEmbedded stopped");
			}
		} finally {
			stopLatch.countDown();
		}
	}
}
