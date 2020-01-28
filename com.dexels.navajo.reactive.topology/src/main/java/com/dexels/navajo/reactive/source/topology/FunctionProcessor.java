package com.dexels.navajo.reactive.source.topology;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;

public class FunctionProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final Function<ReplicationMessage, ReplicationMessage> function;

	private final static Logger logger = LoggerFactory.getLogger(FunctionProcessor.class);

	
	public FunctionProcessor(Function<DataItem,DataItem> func) {
		this.function = func
				.compose(FunctionProcessor::toData)
				.andThen(FunctionProcessor::toReplication);
	}
	

	private static DataItem toData(ReplicationMessage msg) {
		Optional<ImmutableMessage> param = msg.paramMessage();
		if(param.isPresent()) {
			logger.info("DataITEM PARAM: {}",param.get().subMessageListMap());
			return DataItem.of(msg.message(), param.get());
		} else {
			return DataItem.of(msg.message());
		}
		
	}

	private static ReplicationMessage toReplication(DataItem in) {
		ImmutableMessage msg = in.message();
		ImmutableMessage param = in.stateMessage();
		return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), "", System.currentTimeMillis(),Operation.NONE, Collections.emptyList(), msg, Optional.empty(), Optional.of(param));
		
	}

	@Override
	public void process(String key, ReplicationMessage value) {
		if(value==null) {
			logger.warn("Null message to FunctionProcessor. Is this ok? Not propagating.");
			return;
		}
		if(value.operation()!=Operation.DELETE) {
			super.context().forward(key, function.apply(value));
		}
//		log(key,value);
	}

	private void log(String key, ReplicationMessage value) {
		logger.info("FunctionProcessor forwarding key: {} ",key,ReplicationFactory.getInstance().describe(value));
	}
}
