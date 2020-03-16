package com.dexels.navajo.reactive.source.topology;

import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.Function;

public class FunctionProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final Function<ReplicationMessage, ReplicationMessage> function;

	public FunctionProcessor(Function<ReplicationMessage,ReplicationMessage> func) {
		this.function = func;
	}
	@Override
	public void process(String key, ReplicationMessage value) {
		if(value==null) {
			return;
		}
//		if(value.operation()!=Operation.DELETE) {
			super.context().forward(key, function.apply(value));
//		}
	}

}
