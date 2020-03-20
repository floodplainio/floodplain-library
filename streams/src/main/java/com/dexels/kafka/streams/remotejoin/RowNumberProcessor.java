package com.dexels.kafka.streams.remotejoin;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RowNumberProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final String lookupStoreName;
	private KeyValueStore<String, ReplicationMessage> lookupStore;
	
	private final static Logger logger = LoggerFactory.getLogger(RowNumberProcessor.class);

	
	public RowNumberProcessor(String lookupStoreName) {
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
	public void process(String key, ReplicationMessage incoming) {
		if(incoming==null) {
			logger.warn("RowNumber processor does not support deletes yet");
			return;
		}
		ReplicationMessage existing = this.lookupStore.get(key);
		long row;
		if(existing==null) {
			// 1 based
			row =this.lookupStore.approximateNumEntries()+1;
			this.lookupStore.put(key,ReplicationFactory.empty().with("row", row, ImmutableMessage.ValueType.LONG));
		} else {
			row = (long) existing.columnValue("row");
//			this.lookupStore.put(key,ReplicationFactory.empty().with("row", row, "long"));
		}
		context().forward(key,incoming.with("_row", row, ImmutableMessage.ValueType.LONG));
	}

}
