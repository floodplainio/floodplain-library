package com.dexels.kafka.streams.remotejoin;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;

public class ReduceReadProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final String lookupStoreName;
	private KeyValueStore<String, ImmutableMessage> lookupStore;
	private final ImmutableMessage initial;

	public ReduceReadProcessor(String lookupStoreName, ImmutableMessage initial) {
		this.lookupStoreName = lookupStoreName;
		this.initial = initial;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		lookupStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(lookupStoreName);
		super.init(context);
	}

	@Override
	public void process(String key, ReplicationMessage value) {
		ImmutableMessage msg = this.lookupStore.get(StoreStateProcessor.COMMONKEY);
//		System.err.println("Reading reduce. KEy: "+StoreStateProcessor.COMMONKEY+" from lookupstore: "+lookupStoreName);
		if(value==null) {
			// delete
			ImmutableMessage param = msg==null ? initial : msg;
			value = ReplicationFactory.empty().withOperation(Operation.DELETE).withParamMessage(param);
		} else {
			if(msg!=null) {
//				System.err.println("Found: "+msg.toFlatString(ImmutableFactory.createParser()));
				value = value.withParamMessage(msg);
			} else {
				System.err.println("Nothing found.");
				value = value.withParamMessage(initial);
			}
		}
		context().forward(key, value);
		
	}

}
