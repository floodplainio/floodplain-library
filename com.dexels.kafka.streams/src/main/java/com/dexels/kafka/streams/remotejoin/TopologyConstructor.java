package com.dexels.kafka.streams.remotejoin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class TopologyConstructor {
    public final Map<String,List<String>> processorStateStoreMapper = new HashMap<>();
    public final Map<String,StoreBuilder<KeyValueStore<String, ReplicationMessage>>> stateStoreSupplier  = new HashMap<>();
    // TODO: Could be optional, only needed in xml based stream code
    public final Map<String,MessageTransformer> transformerRegistry;
    public final Optional<AdminClient> adminClient;
    public final Set<String> stores = new HashSet<>();
    
    public TopologyConstructor( Map<String,MessageTransformer> transformerRegistry,
    		Optional<AdminClient> adminClient) {
    	this.transformerRegistry = transformerRegistry;
    	this.adminClient = adminClient;
    }
}
