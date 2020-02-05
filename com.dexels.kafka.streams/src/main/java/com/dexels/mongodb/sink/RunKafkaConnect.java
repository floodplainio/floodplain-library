package com.dexels.mongodb.sink;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.api.sink.ConnectConfiguration;
import com.dexels.kafka.streams.base.ConnectSink;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public class RunKafkaConnect implements ConnectSink {
	private static final Logger logger = LoggerFactory.getLogger(RunKafkaConnect.class);
//	private static final int REQUEST_TIMEOUT_MS = 120000;

	private Worker worker;
	private StandaloneHerder herder;
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private final CountDownLatch stopLatch = new CountDownLatch(1);

	OffsetBackingStore offsetBackingStore;
   private Properties[] connectorConfigs;
	private Properties workerProperties;
	private Properties sinkProperties;
	
	public RunKafkaConnect(Optional<XMLElement> x, StreamConfiguration config, TopologyContext topologyContext, AdminClient adminClient, String sinkName, File storageFolder) throws IOException  {
		
		Optional<ConnectConfiguration> sinkConfig = config.connector(sinkName);
		if(!sinkConfig.isPresent()) {
			throw new IllegalArgumentException("No sink found: "+sinkName);
		}
		String generationalGroup = CoreOperators.generationalGroup("connect-"+sinkName, topologyContext);
		String sinkId = generationalGroup;
		
		if(x.isPresent()) {
	        createIndices(x.get().getChildren(), topologyContext,sinkConfig.get());
		}

        Map<String,String> topicMapping = x.isPresent()? sinkTopic(x.get().getChildren(),topologyContext) : Collections.emptyMap();

		Map<String,String> settings = sinkConfig.get().settings();
		workerProperties = new Properties();
		sinkProperties = new Properties();
		sinkProperties.load(RunKafkaConnect.class.getResourceAsStream("sink.properties"));
		workerProperties.load(RunKafkaConnect.class.getResourceAsStream("standalone.properties"));
		sinkProperties.put("name", generationalGroup);
		sinkProperties.put(MongodbSinkConnector.SINKID, sinkId);
		sinkProperties.put(MongodbSinkConnector.HOST, settings.get(MongodbSinkConnector.HOST));
		sinkProperties.put(MongodbSinkConnector.PORT, settings.get(MongodbSinkConnector.PORT));
		String resolvedDatabase = CoreOperators.generationalGroup(settings.get("database"), topologyContext);
		logger.info("Resolved mongo sink definition to {} with generational group {}",resolvedDatabase,generationalGroup);
		sinkProperties.put(MongodbSinkConnector.DATABASE, resolvedDatabase);
		
		topicMapping.entrySet().stream().forEach(e->{
			KafkaUtils.ensureExistsSync(Optional.of(adminClient),e.getKey(),CoreOperators.topicPartitionCount(),CoreOperators.topicReplicationCount());
		});
		sinkProperties.put(MongodbSinkConnector.TOPICS,String.join(",",topicMapping.entrySet().stream().map(e->e.getKey()).collect(Collectors.toList())));
		sinkProperties.put(MongodbSinkConnector.COLLECTIONS,String.join(",",topicMapping.entrySet().stream().map(e->e.getValue()).collect(Collectors.toList())));

		sinkProperties.put("generation",topologyContext.generation);
		sinkProperties.put("instanceName",topologyContext.instance);
		sinkProperties.put("sinkName",sinkName);
		if(topologyContext.tenant.isPresent()) {
			sinkProperties.put("tenant",topologyContext.tenant.get());
		}
		sinkProperties.put("deployment",topologyContext.deployment);

		
		workerProperties.put("bootstrap.servers", String.join(",", config.kafkaHosts()));
		workerProperties.put("group.id", generationalGroup);
		workerProperties.put("compression.type", "lz4");
		
		File output = new File(storageFolder, "mongooffset-"+resolvedDatabase);
		workerProperties.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, output.getAbsolutePath());
		logger.info("Sink: {}", sinkProperties);
		logger.info("Worker: {}", workerProperties);
		setupConfig(topologyContext.instance);
	}
	
	

	private final Map<String,String> sinkTopic(List<XMLElement> topicMapping, TopologyContext topologyContext) {
		Map<String,String> result = new HashMap<>();
		List<String> collections = new ArrayList<>();
		for(XMLElement e :topicMapping) {
			String gencoll = e.getStringAttribute("generationcollection");
			String coll = gencoll!=null?topologyContext.generation+gencoll: e.getStringAttribute("collection");
			collections.add(coll);
			String topicName = CoreOperators.topicName( e.getStringAttribute("topic"),topologyContext);
			logger.info("Connecting topic: {} to mongodb collection: {}",topicName,coll);
			result.put(topicName, coll);
		}
		return Collections.unmodifiableMap(result);
	}
	
	
	private void setupConfig(String instanceName)  {
	    Properties[] connectorConfigs = new Properties[] { this.sinkProperties };
		Time time = Time.SYSTEM;
        logger.info("Kafka Connect standalone worker initializing ...");
//        long initStart = time.hiResClockMs();
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();
		Map<String, String> propsToStringMap = Utils.propsToStringMap(workerProperties);
		
		WorkerConfig config = new StandaloneConfig(propsToStringMap);
		String clusterId = instanceName+"-"+UUID.randomUUID().toString();
		logger.info("Using clusterid: {}",clusterId);

		offsetBackingStore = new FileOffsetBackingStore();
		offsetBackingStore.configure(config);

        Plugins plugins = new Plugins(propsToStringMap);
        plugins.compareAndSwapWithDelegatingLoader();
        worker = new Worker(clusterId, time, plugins, config, offsetBackingStore);
		herder = new StandaloneHerder(worker,clusterId);
		this.connectorConfigs = connectorConfigs; 
	}

	public void start() {
		
			logger.info("Kafka ConnectEmbedded starting. # of connectorConfigs: {}",connectorConfigs.length);
			logger.info("Configs: {}",Utils.propsToStringMap(connectorConfigs[0]));
			offsetBackingStore.start();
			worker.start();
			for (Properties connectorConfig : connectorConfigs) {
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
                    @Override
                    public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                        if (error != null) {
                            logger.error("Failed to create job for {}", connectorConfig);
                            logger.error("Error: ",error);
                        } else {
                        	if(logger.isInfoEnabled()) {
                                logger.info("Created connector {}", info.result().name());
                        	}
                        }
                    }
                });
				
				String name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
                herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, cb);
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
	
	
	private static final void createIndices(List<XMLElement> sinkElements, TopologyContext topologyContext, ConnectConfiguration sinkConfig) {
		if(sinkElements.isEmpty()) {
			return;
		}
		MongoClient client = MongodbSinkTask.createClient(sinkConfig.settings().get(MongodbSinkConnector.HOST), sinkConfig.settings().get(MongodbSinkConnector.PORT));
		String resolvedDatabase = CoreOperators.generationalGroup(sinkConfig.settings().get("database"), topologyContext);

		MongoDatabase db = client.getDatabase(resolvedDatabase);
		for(XMLElement e :sinkElements) {
			String gencoll = e.getStringAttribute("generationcollection");
			String collectionName = gencoll!=null?topologyContext.generation+gencoll: e.getStringAttribute("collection");
			MongoCollection<Document> coll = db.getCollection(collectionName);
			List<XMLElement> indexElements = e.getChildrenByTagName("sink.index");
			for (XMLElement index : indexElements) {
				boolean unique = index.getBooleanAttribute("unique", "true", "false", false);
				boolean sparse = index.getBooleanAttribute("sparse", "true", "false", false);
                String sphereVersion = (String) index.getAttribute("sphereVersion");
				String partialFilterExpression = index.getStringAttribute("partialFilterExpression");
				String definition = index.getStringAttribute("definition");
                logger.info("Creating index for collection: {} with definition: {} unique? {} sparse? {} partial: {} sphereVersion: {}",
                        collectionName, definition, unique, sparse, partialFilterExpression, sphereVersion);
                String result = createIndex(coll, definition, unique, sparse, partialFilterExpression, sphereVersion);
				logger.info("Index result: {}",result);
			}
		}
		client.close();
	}

	
    private static String createIndex(MongoCollection<Document> coll, String definition, boolean unique, boolean sparse, String partial,
            String sphereVersion) {
		List<String> part = Arrays.asList(definition.split(","));
		Document d = new Document();
		
        IndexOptions indexOptions = new IndexOptions();
        if (unique) {
            indexOptions.unique(true);
        }
        if (sparse) {
            indexOptions.sparse(true);
        }
        if (partial != null) {
            try {
                Document partialIndex = Document.parse(partial);
                indexOptions.partialFilterExpression(partialIndex);
            } catch (Throwable t) {
                logger.warn("Invalid document given as partial filter index! {}", partial, t);
            }
        }
        if (sphereVersion != null) {
            indexOptions.sphereVersion(Integer.parseInt(sphereVersion));
        }

		for (String pt : part) {

                List<String> prts = Arrays.asList(pt.split(":"));
                String name = prts.get(0);

            if (prts.get(1).matches("-?\\d+")) {
                int direction = Integer.parseInt(prts.get(1));
                d.put(name, direction);
            } else {
                d.put(name, prts.get(1));
            }

		}



		String searchTerm = definition.replaceAll(",", "_").replaceAll(":", "_");
		for (Document doc : coll.listIndexes()) {
			String name = (String) doc.get("name");
			if(name.equals(searchTerm)) {
				return "";
			}
		}
		logger.info("Created index on collection: {} with definition: {}",coll.getNamespace().getCollectionName(),definition);
		String result = coll.createIndex(d,indexOptions);
		logger.info("Created index result: {}",result);
        return result;
	}

}
