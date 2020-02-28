package com.dexels.kafka.streams.remotejoin;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;

public class StoreStateProcessor extends AbstractProcessor<String, ReplicationMessage> {

	
	private final static Logger logger = LoggerFactory.getLogger(StoreStateProcessor.class);
	private final String name;
	private final String lookupStoreName;
	private final ImmutableMessage initial;
	private KeyValueStore<String, ImmutableMessage> lookupStore;
	public static final String COMMONKEY = "singlerestore";
	public StoreStateProcessor(String name, String lookupStoreName, ImmutableMessage initial) {
		this.name = name;
		this.lookupStoreName = lookupStoreName;
		this.initial = initial;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) {
		
		this.lookupStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(lookupStoreName);
		

		super.init(context);
	}

	@Override
	public void process(String key, ReplicationMessage value) {
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
			ImmutableMessage paramMessage = value.paramMessage().get();
//			paramMessage.with(UUID.randomUUID().toString(), RandomUtils.nextInt(), "integer");
//			l = lookupStore.approximateNumEntries();
//			System.err.println("Number of entries: "+l);
			lookupStore.put(COMMONKEY, paramMessage);
//			l = lookupStore.approximateNumEntries();
//			System.err.println("After number of entries: "+l);
//			System.err.println("Storing key: "+COMMONKEY+" into store: "+lookupStoreName);
//		}
//		ImmutableMessage paramMessage = Optional.ofNullable(lookupStore.get(COMMONKEY)).map(e->e.paramMessage()).orElse(initial);
//		ImmutableMessage msg = Optional.ofNullable(paramMessage.orElse(initial)).orElse(initial);
		
		super.context().forward(COMMONKEY, value);
	}

}
