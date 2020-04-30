package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EachProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final static Logger logger = LoggerFactory.getLogger(EachProcessor.class);
    private final ImmutableMessage.TriConsumer lambda;

    public EachProcessor(ImmutableMessage.TriConsumer lambda) {
        this.lambda = lambda;
    }


    @Override
    public void process(String key, ReplicationMessage value) {
        if(value==null || value.operation()== ReplicationMessage.Operation.DELETE) {
            // do not process delete, just forward
        } else {
            lambda.apply(value.message(), value.paramMessage().orElse(ImmutableFactory.empty()),key);
        }
        super.context().forward(key, value);
    }

}
