package com.dexels.pubsub.rx2.factory.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.dexels.pubsub.rx2.api.PubSubMessage;

public class BasePubSubMessage implements PubSubMessage {

	private final String key;
	private final byte[] value;
	private final Optional<String> topic;
	private final long timestamp;
	private final Optional<Long> offset;
	private final Optional<Integer> partition;
	private Optional<Runnable> commit;
	private final Map<String, byte[]> headers;

	
	public BasePubSubMessage(String key, byte[] value, Optional<String> topic, long timestamp) {
		this(key,value,topic,timestamp,Optional.empty(),Optional.empty(),Optional.empty(),Collections.emptyMap());
	}
	
	public BasePubSubMessage(String key, byte[] value, Optional<String> topic, long timestamp, Optional<Integer> partition, Optional<Long> offset, Optional<Runnable> commit, Map<String, byte[]> headers) {
		this.key = key;
		this.value = value;
		this.topic = topic;
		this.timestamp = timestamp;
		this.partition = partition;
		this.offset = offset;
		this.commit = commit;
		this.headers = headers;
	}
	@Override
	public String key() {
		return key;
	}

	@Override
	public Optional<String> topic() {
		return topic;
	}

	@Override
	public byte[] value() {
		return value;
	}

	@Override
	public long timestamp() {
		return timestamp;
	}

	@Override
	public Optional<Long> offset() {
		return offset;
	}

	@Override
	public Optional<Integer> partition() {
		return partition;
	}

	@Override
	public PubSubMessage withTopic(Optional<String> topic) {
		return new BasePubSubMessage(key, value, topic, timestamp);
	}

	@Override
	public PubSubMessage withKey(String key) {
		return new BasePubSubMessage(key, value, topic, timestamp);
	}

	@Override
	public PubSubMessage withValue(byte[] value) {
		return new BasePubSubMessage(key, value, topic, timestamp);
	}

	@Override
	public PubSubMessage withTimestamp(long timestamp) {
		return new BasePubSubMessage(key, value, topic, timestamp);
	}

	@Override
	public void commit() {
		commit.ifPresent(r->r.run());
	}

	@Override
	public Map<String, byte[]> headers() {
		return this.headers;
	}
}
