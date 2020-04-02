package com.dexels.pubsub.rx2.api;

import java.util.Map;
import java.util.Optional;


public interface PubSubMessage {
    public String key();

    public Optional<String> topic();

    public byte[] value();

    public long timestamp();

    public void commit();

    public PubSubMessage withTopic(Optional<String> topic);

    public PubSubMessage withKey(String key);

    public PubSubMessage withValue(byte[] value);

    public PubSubMessage withTimestamp(long timestamp);

    public Map<String, byte[]> headers();

    default public Optional<Integer> partition() {
        return Optional.empty();
    }

    default public Optional<Long> offset() {
        return Optional.empty();
    }
}
