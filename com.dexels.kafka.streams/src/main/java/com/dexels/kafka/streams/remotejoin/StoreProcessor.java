package com.dexels.kafka.streams.remotejoin;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;

public class StoreProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final String lookupStoreName;
	private KeyValueStore<String, ReplicationMessage> lookupStore;
	
	public StoreProcessor(String lookupStoreName) {
		this.lookupStoreName = lookupStoreName;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(lookupStoreName);
		super.init(context);
	}

	@Override
	public void close() {

	}

	@Override
	public void process(String key, ReplicationMessage outerMessage) {
		if(outerMessage==null || outerMessage.operation()==Operation.DELETE) {
//			logger.info("Delete detected in store: {} with key: {}",lookupStoreName,key);
			ReplicationMessage previous = lookupStore.get(key);
			if(previous!=null) {
				lookupStore.delete(key);
				context().forward(key, previous.withOperation(Operation.DELETE));
			}
			context().forward(key, null);
		} else {
			lookupStore.put(key, outerMessage);
			context().forward(key, outerMessage);
		}
	}

}
