package io.floodplain.pubsub.rx2.factory;

import io.floodplain.pubsub.rx2.api.PubSubMessage;
import io.floodplain.pubsub.rx2.api.TopicSubscriber;
import io.floodplain.pubsub.rx2.factory.impl.BasePubSubMessage;
import io.floodplain.pubsub.rx2.factory.impl.internal.KafkaDumpSubscriber;

import java.io.Reader;
import java.util.Collections;
import java.util.Optional;

public class PubSubTools {
    public static PubSubMessage withTopic(PubSubMessage base, Optional<String> topic) {
        return new BasePubSubMessage(base.key(), base.value(), topic, base.timestamp());
    }

    public static PubSubMessage withKey(PubSubMessage base, String key) {
        return new BasePubSubMessage(base.key(), base.value(), base.topic(), base.timestamp());
    }

    public static PubSubMessage withValue(PubSubMessage base, byte[] value) {
        return new BasePubSubMessage(base.key(), value, base.topic(), base.timestamp());
    }

    public static PubSubMessage withTimestamp(PubSubMessage base, long timestamp) {
        return new BasePubSubMessage(base.key(), base.value(), base.topic(), timestamp);
    }

    public static PubSubMessage create(String key, byte[] value, long timestamp, Optional<String> topic) {
        return new BasePubSubMessage(key, value, topic, timestamp);
    }

    public static PubSubMessage create(String key, byte[] value, long timestamp, Optional<String> topic, Optional<Integer> partition, Optional<Long> offset) {
        return new BasePubSubMessage(key, value, topic, timestamp, partition, offset, Optional.empty(), Collections.emptyMap());
    }

    public static TopicSubscriber createMockSubscriber(Reader reader) {
        return new KafkaDumpSubscriber(reader);
    }
}
