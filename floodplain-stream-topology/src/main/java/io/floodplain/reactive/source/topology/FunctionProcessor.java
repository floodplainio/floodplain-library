package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.BiFunction;

public class FunctionProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final SetTransformer.TriFunction function;

    public FunctionProcessor(SetTransformer.TriFunction func) {
        this.function = func;
    }

    @Override
    public void process(String key, ReplicationMessage value) {
        if (value == null) {
            // forward nulls unchanged
            super.context().forward(key,null);
            return;
        }
        ReplicationMessage.Operation operation = value.operation();

//		if(value.operation()!= ReplicationMessage.Operation.DELETE) {
            ImmutableMessage applied = function.apply(key,value.message(), value.paramMessage().orElse(ImmutableFactory.empty()));
            super.context().forward(key, ReplicationFactory.standardMessage(applied).withParamMessage(value.paramMessage().orElse(ImmutableFactory.empty())).withOperation(operation));
//		} else {
//            super.context().forward(key, value);
//        }
    }

}
