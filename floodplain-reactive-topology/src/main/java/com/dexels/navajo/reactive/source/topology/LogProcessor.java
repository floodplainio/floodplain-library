package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final static Logger logger = LoggerFactory.getLogger(LogProcessor.class);
	private final String name;
	private final boolean dumpStack;
	private long invocationCounter = 0;
	public LogProcessor(String name, boolean dumpStack) {
		this.name = name;
		this.dumpStack = dumpStack;
	}
	
	@Override
	public void process(String key, ReplicationMessage value) {
		if (value!=null) {
			logger.info("Processing processor: {} invocationNumber: {} with key: {} value: \n{}",this.name,invocationCounter++,key,ReplicationFactory.getInstance().describe(value));
			if(value.paramMessage().isPresent()) {
				ImmutableMessage param = value.paramMessage().get();
				logger.info("Processing immutablemessage in processor: {} : {}",this.name,ImmutableFactory.getInstance().describe(param));
			}
		} else {
			logger.info("Processing processor: {} with key: {} value: <none> invocationNumber: {}",this.name,key,invocationCounter++);
		}
		if(dumpStack) {
			logger.warn("Dumping stack for logger",new Exception());
		}
		super.context().forward(key, value);
	}

}
