package com.dexels.kafka.streams.remotejoin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class TopologyConstructor {
    public final Map<String,List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String,StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier  = new HashMap<>();
    public final Map<String,MessageTransformer> transformerRegistry;
    public final AdminClient adminClient;
    
    public TopologyConstructor( Map<String,MessageTransformer> transformerRegistry,
    		AdminClient adminClient) {
    	this.transformerRegistry = transformerRegistry;
    	this.adminClient = adminClient;
    }
}
