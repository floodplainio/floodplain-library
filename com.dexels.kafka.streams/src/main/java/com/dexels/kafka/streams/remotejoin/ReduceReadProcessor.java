package com.dexels.kafka.streams.remotejoin;

import java.util.Optional;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;

public class ReduceReadProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final String accumulatorStoreName;
	private KeyValueStore<String, ImmutableMessage> accumulatorStore;
	private KeyValueStore<String, ReplicationMessage> lookupStore;
	private final ImmutableMessage initial;
	private final Optional<ContextExpression> keyExtractor;

	public ReduceReadProcessor(String accumulatorStoreName, ImmutableMessage initial, Optional<ContextExpression> keyExtractor) {
		this.accumulatorStoreName = accumulatorStoreName;
		this.initial = initial;
		this.keyExtractor = keyExtractor;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		accumulatorStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(accumulatorStoreName);
		super.init(context);
	}

	@Override
	public void process(String key, final ReplicationMessage inputValue) {
		Optional<String> extracted = keyExtractor.map(e->e.apply(null,Optional.of(inputValue.message()),inputValue.paramMessage())).map(e->(String)e.value);
		System.err.println("KEY: "+extracted);
		ImmutableMessage msg = this.accumulatorStore.get(extracted.orElse(StoreStateProcessor.COMMONKEY));
		ReplicationMessage value = inputValue;
		if(inputValue==null) {
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
