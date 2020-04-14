package io.floodplain.kafka.factory;

import io.floodplain.kafka.impl.KafkaTopicPublisher;
import io.floodplain.kafka.impl.KafkaTopicSubscriber;
import io.floodplain.pubsub.rx2.api.PersistentPublisher;
import io.floodplain.pubsub.rx2.api.PersistentSubscriber;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class KafkaClientFactory {
    public static PersistentSubscriber createSubscriber(String hosts, Map<String, String> config) {
        KafkaTopicSubscriber ksi = new KafkaTopicSubscriber();
        Map<String, Object> settings = new HashMap<>();
        settings.put("hosts", hosts);
        settings.put("max", "1000");
        settings.put("wait", "1000");
        settings.putAll(config);
        ksi.activate(settings);
        return ksi;
    }

    public static PersistentPublisher createPublisher(String hosts, int partitions, int replicationFactor) {
        KafkaTopicPublisher kpi = new KafkaTopicPublisher();
        Map<String, Object> settings = new HashMap<>();
        settings.put("hosts", hosts);
        settings.put("replicationFactor", "" + replicationFactor);
        settings.put("client.id", UUID.randomUUID().toString());
        settings.put("retries", "30");
        settings.put("partitions", "" + partitions);
        kpi.activate(settings);
        return kpi;
    }
}
