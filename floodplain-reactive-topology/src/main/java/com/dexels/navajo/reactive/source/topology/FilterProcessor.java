package com.dexels.navajo.reactive.source.topology;

import java.util.Optional;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.dexels.navajo.document.Operand;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.replication.api.ReplicationMessage;

public class FilterProcessor extends AbstractProcessor<String, ReplicationMessage> {

	private final ContextExpression filterExpression;
	
	public FilterProcessor(ContextExpression filterExpression) {
		this.filterExpression = filterExpression;
	}
	@Override
	public void process(String key, ReplicationMessage value) {
		Operand o = filterExpression.apply(null, Optional.of(value.message()), value.paramMessage());
		if(o.booleanValue()) {
			super.context().forward(key, value);
		}
	}

}
