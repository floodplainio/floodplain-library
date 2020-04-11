package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.BiFunction;

public class FunctionProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final BiFunction<ImmutableMessage, ImmutableMessage, ImmutableMessage> function;

    public FunctionProcessor(BiFunction<ImmutableMessage, ImmutableMessage, ImmutableMessage> func) {
        this.function = func;
    }

    @Override
    public void process(String key, ReplicationMessage value) {
        if (value == null) {
            return;
        }
//		if(value.operation()!=Operation.DELETE) {
        ImmutableMessage applied = function.apply(value.message(), value.paramMessage().orElse(ImmutableFactory.empty()));
        super.context().forward(key, ReplicationFactory.standardMessage(applied).withParamMessage(value.paramMessage().orElse(ImmutableFactory.empty())));
//		}
    }

}
