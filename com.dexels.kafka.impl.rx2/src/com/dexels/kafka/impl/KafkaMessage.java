package com.dexels.kafka.impl;

import java.util.Optional;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.factory.PubSubTools;

public class KafkaMessage implements PubSubMessage {

	private final String key;
	private final byte[] value;

	private final long timestamp;
	private final String topic;
	private long offset;
	private int partition;
	private final BiConsumer<TopicPartition, Long> committer;

	public KafkaMessage(ConsumerRecord<String,byte[]> record,BiConsumer<TopicPartition,Long> committer) {
		this.topic = record.topic();
		this.key = record.key();
		this.value = record.value();
		this.timestamp = record.timestamp();
		this.offset = record.offset();
		this.partition = record.partition();
		this.committer = committer;
	}

	@Override
	public String key() {
		return key;
	}

	@Override
	public byte[] value() {
		return value;
	}
	
	@Override
	public Optional<String> topic() {
		return Optional.of(topic);
	}
	
	@Override
	public long timestamp() {
		return timestamp;
	}
	
	public String toString() {
		if(value==null) {
			return "null";
		} return new String(value);
	}

	@Override
	public PubSubMessage withTopic(Optional<String> topic) {
		return PubSubTools.withTopic(this, topic);
	}

	@Override
	public PubSubMessage withKey(String withKey) {
		return PubSubTools.withKey(this, withKey);
	}

	@Override
	public PubSubMessage withValue(byte[] value) {
		return PubSubTools.withValue(this, value);
	}

	@Override
	public PubSubMessage withTimestamp(long timestamp) {
		return PubSubTools.withTimestamp(this, timestamp);
	}

	@Override
	public Optional<Integer> partition() {
		return Optional.of(partition);
	}

	@Override
	public Optional<Long> offset() {
		return Optional.of(offset);
	}

	@Override
	public void commit() {
		this.committer.accept(new TopicPartition(topic, partition), offset);		
	}

}
