package com.dexels.kafka.streams.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

public class ReplicationMessageSerde implements Serde<ReplicationMessage> {

	private final FallbackReplicationMessageParser parser = new FallbackReplicationMessageParser();

	public ReplicationMessageSerde() {
		ReplicationFactory.setInstance(parser);
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public Deserializer<ReplicationMessage> deserializer() {
		return new Deserializer<ReplicationMessage>() {

			@Override
			public void close() {
			}

			@Override
			public void configure(Map<String, ?> config, boolean isKey) {
				System.err.println("Configuring deserializer: "+config);
				
			}

			@Override
			public ReplicationMessage deserialize(String topic, byte[] data) {
				return parser.parseBytes(data);
			}
		};
	}

	@Override
	public Serializer<ReplicationMessage> serializer() {
		return new Serializer<ReplicationMessage>() {

			@Override
			public void close() {
				
			}

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {
				System.err.println("Configuring: "+configs);
			}

			@Override
			public byte[] serialize(String topic, ReplicationMessage data) {
				if(data==null) {
					return null;
				}
				return data.toBytes(parser);
			}
		};
	}

}
