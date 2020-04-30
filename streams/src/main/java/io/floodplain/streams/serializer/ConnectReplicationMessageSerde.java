package io.floodplain.streams.serializer;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.impl.json.ReplicationJSON;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

public class ConnectReplicationMessageSerde implements Serde<ReplicationMessage> {


    private static final Logger logger = LoggerFactory.getLogger(ConnectReplicationMessageSerde.class);

    public ConnectReplicationMessageSerde() {
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
                logger.info("Configuring deserializer: {}", config);

            }

            @Override
            public ReplicationMessage deserialize(String topic, byte[] data) {
                throw new UnsupportedOperationException("TODO");
            }
        };
    }

    @Override
    public Serializer<ReplicationMessage> serializer() {
        return new Serializer<ReplicationMessage>() {

            private boolean isKey;

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                logger.info("Configuring: {}", configs);
                this.isKey = isKey;
            }

            @Override
            public byte[] serialize(String topic, ReplicationMessage data) {
                if (data == null) {
                    return null;
                }
                if (isKey) {
                    return data.primaryKeys().stream().collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR)).getBytes();
                } else {
                    byte[] replicationToConnectJSON = ReplicationJSON.replicationToConnectJSON(data);
                    return replicationToConnectJSON;

                }
            }
        };
    }

}
