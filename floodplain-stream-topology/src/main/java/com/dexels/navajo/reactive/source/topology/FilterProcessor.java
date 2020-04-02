package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.BiFunction;

public class FilterProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final BiFunction<ImmutableMessage, ImmutableMessage, Boolean> filterExpression;

    public FilterProcessor(BiFunction<ImmutableMessage, ImmutableMessage, Boolean> func) {
        this.filterExpression = func;
    }

    @Override
    public void process(String key, ReplicationMessage value) {

//		Operand o = filterExpression.apply(Optional.of(value.message()), value.paramMessage());
        if (filterExpression.apply(value.message(), value.paramMessage().orElse(ImmutableFactory.empty()))) {
            super.context().forward(key, value);
        }
    }

}
