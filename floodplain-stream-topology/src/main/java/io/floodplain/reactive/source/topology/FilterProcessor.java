package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.BiFunction;

public class FilterProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final BiFunction<String,ImmutableMessage, Boolean> filterExpression;

    public FilterProcessor(BiFunction<String,ImmutableMessage, Boolean> func) {
        this.filterExpression = func;
    }

    @Override
    public void process(String key, ReplicationMessage value) {

//		Operand o = filterExpression.apply(Optional.of(value.message()), value.paramMessage());
        if (filterExpression.apply(key,value.message())) {
            super.context().forward(key, value);
        }
    }

}
