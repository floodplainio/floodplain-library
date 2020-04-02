package com.dexels.kafka.streams.serializer;

import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ReplicationMessageDeserializer implements org.apache.kafka.common.serialization.Deserializer<ReplicationMessage> {
    private Deserializer<ReplicationMessage> deserializer = new ReplicationMessageSerde().deserializer();

    @Override
    public void close() {
        deserializer.close();

    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        deserializer.configure(arg0, arg1);

    }

    @Override
    public ReplicationMessage deserialize(String arg0, byte[] arg1) {
        return deserializer.deserialize(arg0, arg1);
    }

}
