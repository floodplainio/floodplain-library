package com.dexels.kafka.streams.debezium.impl;

import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.Optional;

public class PubSubTopicNameExtractor implements TopicNameExtractor<String, PubSubMessage> {

    private final TopologyConstructor topologyConstructor;

    public PubSubTopicNameExtractor(TopologyConstructor topologyConstructor) {
        this.topologyConstructor = topologyConstructor;
    }

    @Override
    public String extract(String key, PubSubMessage msg, RecordContext context) {
        String result = msg.topic().orElse(context.topic());
        System.err.println("TOPICNAME extracted: " + result);

        if (!this.topologyConstructor.topics.contains(result)) {
            KafkaUtils.ensureExistsSync(topologyConstructor.adminClient, result, Optional.empty());
            topologyConstructor.topics.add(result);
        }

        return result;
    }

}
