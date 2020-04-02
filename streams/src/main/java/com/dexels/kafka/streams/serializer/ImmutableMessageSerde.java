package com.dexels.kafka.streams.serializer;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.impl.json.ReplicationJSON;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class ImmutableMessageSerde implements Serde<ImmutableMessage> {

//	private final FallbackReplicationMessageParser parser = new FallbackReplicationMessageParser();

    public ImmutableMessageSerde() {
//		ReplicationFactory.setInstance(parser);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Deserializer<ImmutableMessage> deserializer() {
        return new Deserializer<ImmutableMessage>() {

            @Override
            public void close() {
            }

            @Override
            public void configure(Map<String, ?> config, boolean isKey) {

            }

            @Override
            public ImmutableMessage deserialize(String topic, byte[] data) {
                try {
                    return ReplicationJSON.parseImmutable(data);
                } catch (IOException e) {
                    throw new RuntimeException("Error parsing json immutable:", e);
                }
            }
        };
    }

    @Override
    public Serializer<ImmutableMessage> serializer() {
        return new Serializer<ImmutableMessage>() {

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, ImmutableMessage data) {
                if (data == null) {
                    return null;
                }
                return ReplicationJSON.immutableTotalToJSON(data);
            }
        };
    }

}
