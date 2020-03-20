package com.dexels.navajo.reactive.source.topology;

import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.Optional;

public class FilterProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final ContextExpression filterExpression;
	
	public FilterProcessor(ContextExpression filterExpression) {
		this.filterExpression = filterExpression;
	}
	@Override
	public void process(String key, ReplicationMessage value) {
		Operand o = filterExpression.apply(Optional.of(value.message()), value.paramMessage());
		if(o.booleanValue()) {
			super.context().forward(key, value);
		}
	}

}
