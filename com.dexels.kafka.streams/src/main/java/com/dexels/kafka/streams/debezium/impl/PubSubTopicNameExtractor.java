package com.dexels.kafka.streams.debezium.impl;

import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import com.dexels.pubsub.rx2.api.PubSubMessage;

public class PubSubTopicNameExtractor implements TopicNameExtractor<String, PubSubMessage> {

	@Override
	public String extract(String key, PubSubMessage msg, RecordContext context) {
		return msg.topic().orElse(context.topic());
	}

}
