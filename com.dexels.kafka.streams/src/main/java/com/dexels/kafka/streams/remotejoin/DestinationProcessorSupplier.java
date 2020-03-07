package com.dexels.kafka.streams.remotejoin;

import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


public class DestinationProcessorSupplier implements ProcessorSupplier<String, ReplicationMessage> {

	private final Function<ReplicationMessage, String> keyExtract;
	private final Predicate<String, ReplicationMessage> filter;
	private final AtomicLong matched = new AtomicLong();
	private final AtomicLong totalMatched = new AtomicLong();

	public DestinationProcessorSupplier(Function<ReplicationMessage, String> keyExtract,
			Predicate<String, ReplicationMessage> destinationFilter) {
		this.keyExtract = keyExtract;
		this.filter = destinationFilter;
	}
	@Override
	public Processor<String, ReplicationMessage> get() {
		return new AbstractProcessor<String, ReplicationMessage>() {

			@Override
			public void process(String key, ReplicationMessage msg) {
				if(msg==null) {
					// ignore null messages, deletes should have been converted into Operation.DELETE messages by the store
					return;
				}
				if(filter.test(key, msg)) {
					context().forward(keyExtract.apply(msg), msg);
					matched.incrementAndGet();
				}
				totalMatched.incrementAndGet();
//				logger.info("Split Destination to {} matched ({}/{})",DestinationProcessorSupplier.this.topic,matched.get(),totalMatched.get());
			}
		};
	}

}
