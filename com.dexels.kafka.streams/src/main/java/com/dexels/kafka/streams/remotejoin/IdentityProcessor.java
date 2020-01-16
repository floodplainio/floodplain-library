package com.dexels.kafka.streams.remotejoin;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.dexels.replication.api.ReplicationMessage;

public class IdentityProcessor extends AbstractProcessor<String, ReplicationMessage> {

	@Override
	public void process(String key, ReplicationMessage value) {
		super.context().forward(key, value);
	}

}
