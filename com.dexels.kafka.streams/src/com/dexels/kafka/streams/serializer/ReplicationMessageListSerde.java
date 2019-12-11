package com.dexels.kafka.streams.serializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

public class ReplicationMessageListSerde implements Serde<List<ReplicationMessage>> {

	private final FallbackReplicationMessageParser parser = new FallbackReplicationMessageParser();

	public ReplicationMessageListSerde() {
		ReplicationFactory.setInstance(parser);
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		System.err.println("Configuring: "+configs);
	}

	@Override
	public Deserializer<List<ReplicationMessage>> deserializer() {
		return new Deserializer<List<ReplicationMessage>>() {

			@Override
			public void close() {
			}

			@Override
			public void configure(Map<String, ?> config, boolean isKey) {
				
			}

			@Override
			public List<ReplicationMessage> deserialize(String topic, byte[] data) {
				if(data==null) {
					return Collections.emptyList();
				}
				return parser.parseMessageList(data);
			}
		};
	}

	@Override
	public Serializer<List<ReplicationMessage>> serializer() {
		return new Serializer<List<ReplicationMessage>>() {

			@Override
			public void close() {
				
			}

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
				
			}

			@Override
			public byte[] serialize(String topic, List<ReplicationMessage> data) {
				
				return parser.serializeMessageList(data);
				

			}
		};
	}

}
