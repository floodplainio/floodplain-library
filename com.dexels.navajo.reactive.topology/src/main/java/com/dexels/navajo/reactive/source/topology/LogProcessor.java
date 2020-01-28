package com.dexels.navajo.reactive.source.topology;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;

public class LogProcessor extends AbstractProcessor<String, ReplicationMessage> {

	
	private final static Logger logger = LoggerFactory.getLogger(LogProcessor.class);
	private final String name;

	public LogProcessor(String name) {
		this.name = name;
	}
	
	@Override
	public void process(String key, ReplicationMessage value) {
		if (value!=null) {
			logger.info("Processing processor: {} with key: {} value: \n{}",this.name,key,ReplicationFactory.getInstance().describe(value));
			if(value.paramMessage().isPresent()) {
				ImmutableMessage param = value.paramMessage().get();
				logger.info("Processing immutablemessage in processor: {} : {}",this.name,ImmutableFactory.getInstance().describe(param));
				
			}
		} else {
			logger.info("Processing processor: {} with key: {} value: <none>",this.name,key);
		}
		super.context().forward(key, value);
	}

}
