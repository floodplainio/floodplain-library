package com.dexels.kafka.factory;

import com.dexels.kafka.impl.KafkaTopicPublisher;
import com.dexels.kafka.impl.KafkaTopicSubscriber;
import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import org.osgi.annotation.versioning.ProviderType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@ProviderType
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
