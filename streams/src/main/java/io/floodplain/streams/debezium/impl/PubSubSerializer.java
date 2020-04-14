package io.floodplain.streams.debezium.impl;

import io.floodplain.pubsub.rx2.api.PubSubMessage;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PubSubSerializer implements Serializer<PubSubMessage> {

    @Override
    public void close() {
        // no op
    }

    @Override
    public void configure(Map<String, ?> config, boolean arg1) {

    }

    @Override
    public byte[] serialize(String arg0, PubSubMessage msg) {
        return msg.value();
    }

}
