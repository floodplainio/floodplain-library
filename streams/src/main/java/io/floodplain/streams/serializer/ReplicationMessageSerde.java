package io.floodplain.streams.serializer;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ReplicationMessageSerde implements Serde<ReplicationMessage> {

    private final FallbackReplicationMessageParser parser = new FallbackReplicationMessageParser();
    private static final Logger logger = LoggerFactory.getLogger(ReplicationMessageSerde.class);

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
                logger.info("Configuring deserializer: {}",config);

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
                logger.info("Configuring: {}", configs);
            }

            @Override
            public byte[] serialize(String topic, ReplicationMessage data) {
                if (data == null) {
                    return null;
                }
                return data.toBytes(parser);
            }
        };
    }

}
