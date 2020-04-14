package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
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
