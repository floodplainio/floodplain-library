package com.dexels.kafka.streams.remotejoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class TopologyConstructor {
	
	public class ConnectorTopicTuple {
		public final String connectorResourceName;
		public final Map<String, String> sinkParameters;
		public final String topicName;
		
		public ConnectorTopicTuple(String connectorResourceName, String topicName, Map<String, String> sinkParameters) {
			this.connectorResourceName = connectorResourceName;
			this.sinkParameters = sinkParameters;
			this.topicName = topicName;
		}
	}

	public final Map<String,List<ConnectorTopicTuple>> connectorAssociations = new HashMap<>();
//	public final List<ConnectorTopicTuple> connectorAssociations = new LinkedList<>();
    public final Map<String,List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String,StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier  = new HashMap<>();
    // TODO: Could be optional, only needed in xml based stream code
    public final Map<String,MessageTransformer> transformerRegistry;
    public final Optional<AdminClient> adminClient;
    public final Set<String> stores = new HashSet<>();
//    public final Set<String> processors = new HashSet<>();
    public final Map<String,String> sources = new HashMap<>();
    // TODO could race conditions happen? If so, would that be a problem?
    public final Set<String> topics = new HashSet<String>();
    public TopologyConstructor( Optional<Map<String,MessageTransformer>> transformerRegistry,
    		Optional<AdminClient> adminClient) {
    	this.transformerRegistry = transformerRegistry.orElse(Collections.emptyMap());
    	this.adminClient = adminClient;
    	if(this.adminClient.isPresent()) {
    		try {
				topics.addAll(adminClient.get().listTopics().names().get());
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException("Error listing topics", e);
			}
    	}
    }
    
    public void ensureTopicExists(String topicName, Optional<Integer> partitionCount) {
    	if(topics.contains(topicName)) {
    		return;
    	}
    	KafkaUtils.ensureExistsSync(adminClient, topicName,partitionCount);
    }
    
    public void addConnectSink(String connectorResourceName, String topicName, Map<String,String> sinkParameters) {
    	List<ConnectorTopicTuple> ctt =connectorAssociations.compute(connectorResourceName, (resourceName,list)->list==null?new ArrayList<>():list);
    	ctt.add(new ConnectorTopicTuple(connectorResourceName,topicName,sinkParameters));
    }
    
    public Set<String> detectedConnectors() {
    	return connectorAssociations.keySet(); //  stream().map(e->e.connectorResourceName).collect(Collectors.toSet());
    }
//    
//    public Set<String> topicsForConnector(String connectorName) {
//    	return connectorAssociations.stream().filter(e->e.connectorResourceName.equals(connectorName)).map(e->e.topicName).collect(Collectors.toSet());
//    }

}
