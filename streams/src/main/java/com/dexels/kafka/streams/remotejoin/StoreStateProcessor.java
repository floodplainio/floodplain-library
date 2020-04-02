package com.dexels.kafka.streams.remotejoin;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiFunction;

public class StoreStateProcessor extends AbstractProcessor<String, ReplicationMessage> {

	
	private final static Logger logger = LoggerFactory.getLogger(StoreStateProcessor.class);
	private final String name;
	private final String lookupStoreName;
	private final ImmutableMessage initial;
	private KeyValueStore<String, ImmutableMessage> lookupStore;
	private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;
	public static final String COMMONKEY = "singlerestore";
	public StoreStateProcessor(String name, String lookupStoreName, ImmutableMessage initial, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {
		this.name = name;
		this.lookupStoreName = lookupStoreName;
		this.initial = initial;
		this.keyExtractor = keyExtractor;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		
		this.lookupStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(lookupStoreName);
		

		super.init(context);
	}

	@Override
	public void process(String key, ReplicationMessage inputValue) {

		String extracted = keyExtractor.orElse((msg,state)->COMMONKEY).apply(inputValue.message(),inputValue.paramMessage().orElse(ImmutableFactory.empty())); //  keyExtractor.map(e->e.apply(Optional.of(inputValue.message()),inputValue.paramMessage())).map(e->(String)e.value);

		//		for (int i = 0; i < 50; i++) {
//			lookupStore.put(""+i, ImmutableFactory.empty().with("number", i, "integer").with("aap","noot", "string"));
//		}

//		ImmutableMessage vl = lookupStore.get(COMMONKEY);
//		System.err.println("State store reduce. Key: "+COMMONKEY);
//		long l = lookupStore.approximateNumEntries();
//		System.err.println("|||>> GET: "+key+" store: "+lookupStoreName+" hash: "+store.hashCode()+" # of entries: "+l);
//		if(vl==null) {
//			System.err.println("not Found. # of entries: "+l);
//			throw new RuntimeException("Missing value for key: "+COMMONKEY);
//			vl = initial;
//			lookupStore.put(COMMONKEY, vl);
//			l = lookupStore.approximateNumEntries();
//			System.err.println("not Found. # of entries now: "+l);
//		} else {
//			System.err.println("Found: "+vl.toFlatString(ImmutableFactory.createParser()));
//			System.err.println("Input msg: "+value.message().toFlatString(ImmutableFactory.createParser()));
//			System.err.println("Input param: "+value.paramMessage().orElse(ImmutableFactory.empty()).toFlatString(ImmutableFactory.createParser()));
			ImmutableMessage paramMessage = inputValue.paramMessage().get();
//			paramMessage.with(UUID.randomUUID().toString(), RandomUtils.nextInt(), "integer");
//			l = lookupStore.approximateNumEntries();
//			System.err.println("Number of entries: "+l);
			lookupStore.put(extracted, paramMessage);
//			l = lookupStore.approximateNumEntries();
//			System.err.println("After number of entries: "+l);
//			System.err.println("Storing key: "+COMMONKEY+" into store: "+lookupStoreName);
//		}
//		ImmutableMessage paramMessage = Optional.ofNullable(lookupStore.get(COMMONKEY)).map(e->e.paramMessage()).orElse(initial);
//		ImmutableMessage msg = Optional.ofNullable(paramMessage.orElse(initial)).orElse(initial);
//		String keyVal = this.key.map(e->)
//		super.context().forward(extracted.orElse(COMMONKEY), inputValue);
		super.context().forward(extracted, inputValue.withOperation(Operation.UPDATE));
	}

}
