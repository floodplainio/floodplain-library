package io.floodplain.streams.serializer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ConnectKeySerializer implements Serializer<String> {

    private static final Logger logger = LoggerFactory.getLogger(ConnectKeySerializer.class);

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        logger.info("Configuring: {}", configs);
    }

    @Override
    public byte[] serialize(String topic, String key) {
        String converted = "{\"key\":\"" + key + "\" }";
        return converted.getBytes(StandardCharsets.UTF_8);
    }
}
