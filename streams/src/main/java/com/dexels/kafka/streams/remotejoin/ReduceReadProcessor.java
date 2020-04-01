package com.dexels.kafka.streams.remotejoin;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;
import java.util.function.BiFunction;

public class ReduceReadProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final String accumulatorStoreName;
	private final String inputStoreName;
	private KeyValueStore<String, ImmutableMessage> accumulatorStore;
//	private KeyValueStore<String, ReplicationMessage> lookupStore;
	private final ImmutableMessage initial;
	private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;
	private KeyValueStore<String, ReplicationMessage> inputStore;

	public ReduceReadProcessor(String inputStoreName, String accumulatorStoreName, ImmutableMessage initial, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {
		this.accumulatorStoreName = accumulatorStoreName;
		this.inputStoreName = inputStoreName;
		this.initial = initial;
		this.keyExtractor = keyExtractor;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		accumulatorStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(accumulatorStoreName);
		inputStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(inputStoreName);
		super.init(context);
	}

	@Override
	public void process(String key, final ReplicationMessage inputValue) {
		ReplicationMessage stored = inputStore.get(key);
		String extracted;
		if(stored==null) {
			// no stored value, so must be upsert.
			if(inputValue==null || inputValue.operation()==Operation.DELETE) {
				throw new RuntimeException("Issue: Deleting (?) a message that isn't there. Is this bad?");
			}
			extracted = keyExtractor.orElse((m,s)->StoreStateProcessor.COMMONKEY) .apply(inputValue.message(),inputValue.paramMessage().orElse(ImmutableFactory.empty()));
//			extracted = keyExtractor.map(e->e.apply(Optional.of(inputValue.message()),inputValue.paramMessage())).map(e->(String)e.value);
		} else {
			extracted = keyExtractor.orElse((m,s)->StoreStateProcessor.COMMONKEY).apply(stored.message(),stored.paramMessage().orElse(ImmutableFactory.empty())	);
//			extracted = keyExtractor.map(e->e.apply(Optional.of(stored.message()),stored.paramMessage())).map(e->(String)e.value);
			
		}
//		Optional<String> extracted = keyExtractor.map(e->e.apply(null,Optional.of(stored.message()),stored.paramMessage())).map(e->(String)e.value);
//		System.err.println("KEY: "+extracted);
		ImmutableMessage msg = this.accumulatorStore.get(extracted);
		ReplicationMessage value = inputValue;
		inputStore.put(key, inputValue);
		if(inputValue==null || inputValue.operation()==Operation.DELETE) {
			if(stored==null) {
				throw new RuntimeException("Issue: Deleting a message that isn't there. Is this bad?");
			}
			// delete
			ImmutableMessage param = msg==null ? initial : msg;
			value = stored.withOperation(Operation.DELETE).withParamMessage(param);
			inputStore.delete(key);
		} else {
			value = value.withParamMessage(msg!=null?msg:initial);
			if(stored!=null) {
				// already present, propagate old value first as delete
				context().forward(key, stored.withOperation(Operation.DELETE).withParamMessage(msg!=null?msg:initial));
				System.err.println("Forwarding update: "+key);
				msg = this.accumulatorStore.get(extracted);
			}
			value = value.withParamMessage(msg!=null?msg:initial);
		}
		context().forward(key, value);
		
	}

}
