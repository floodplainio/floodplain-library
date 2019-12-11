package com.dexels.kafka.streams.base;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.serializer.ReplicationMessageListSerde;
import com.dexels.kafka.streams.serializer.ReplicationMessageSerde;
import com.dexels.kafka.streams.xml.XmlMessageTransformerImpl;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class StreamOperators {
	
	public static final int DEFAULT_MAX_LIST_SIZE = 500;
	public static final ReplicationMessageSerde replicationSerde = new ReplicationMessageSerde();
	public static final ReplicationMessageListSerde replicationListSerde = new ReplicationMessageListSerde();


	private static final Logger logger = LoggerFactory.getLogger(StreamOperators.class);
		
	private StreamOperators() {
		// -- no instances
	}
	
	public static Optional<MessageTransformer> transformersFromChildren(Optional<XMLElement> parentElement, Map<String,MessageTransformer> transformerRegistry, String sourceTopicName) {
		if(!parentElement.isPresent()) {
			return Optional.empty();
		}
		XMLElement parent = parentElement.get();
		if(parent.getChildren()==null || parent.getChildren().isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new XmlMessageTransformerImpl(transformerRegistry, parent,sourceTopicName));
	}
	

	public static StoreBuilder<KeyValueStore<String, ReplicationMessage>> createMessageStoreSupplier(String name) {
		logger.info("Creating messagestore supplier: {}",name);
		KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(name);
		
		return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), replicationSerde)
				.withCachingEnabled();

	}

}
