package com.dexels.mongodb.sink;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.ConnectSink;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticSink implements ConnectSink {
	private static final Logger logger = LoggerFactory.getLogger(ElasticSink.class);
//	private static final int REQUEST_TIMEOUT_MS = 120000;

	private Worker worker;
	private StandaloneHerder herder;
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private final CountDownLatch stopLatch = new CountDownLatch(1);

	OffsetBackingStore offsetBackingStore;
   private Properties[] connectorConfigs;
	private Properties workerProperties;
	private Properties sinkProperties;
	
	public ElasticSink(Optional<XMLElement> x, StreamConfiguration config, TopologyContext context, String sinkName, File storageFolder) throws IOException  {
		

	}
	
	

	private final Map<String,String> sinkTopic(List<XMLElement> topicMapping, TopologyContext topologyContext) {
		Map<String,String> result = new HashMap<>();
		List<String> collections = new ArrayList<>();
		for(XMLElement e :topicMapping) {
			String gencoll = e.getStringAttribute("generationcollection");
			String coll = gencoll!=null?topologyContext.generation+gencoll: e.getStringAttribute("collection");
			collections.add(coll);
			String topicName = CoreOperators.topicName(e.getStringAttribute("topic"),topologyContext);
			logger.info("Connecting topic: {} to mongodb collection: {}",topicName,coll);
			result.put(topicName, coll);
		}
		return Collections.unmodifiableMap(result);
	}

	public void start() {
 		
			logger.info("Kafka ConnectEmbedded starting");
			offsetBackingStore.start();
			worker.start();
			for (Properties connectorConfig : connectorConfigs) {
				new FutureCallback<>();
                String name = connectorConfig.getProperty(ConnectorConfig.NAME_CONFIG);
                herder.putConnectorConfig(name, Utils.propsToStringMap(connectorConfig), true, (a,b) -> {
                    logger.info("Config created: {}", name);
                    
                });
		    }   
			
		
	}

	@Override
	public void shutdown() {
	}

}
