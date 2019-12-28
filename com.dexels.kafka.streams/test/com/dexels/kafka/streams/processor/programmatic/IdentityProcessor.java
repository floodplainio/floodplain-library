package com.dexels.kafka.streams.processor.programmatic;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.dexels.replication.api.ReplicationMessage;

public class IdentityProcessor extends AbstractProcessor<String, ReplicationMessage> {

	@Override
	public void process(String key, ReplicationMessage message) {
		// noop
		super.context().forward(key, message);
	}

}
