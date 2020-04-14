package io.floodplain.streams.remotejoin;

import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

public class IfElseProcessor extends AbstractProcessor<String, ReplicationMessage> {


    private final static Logger logger = LoggerFactory.getLogger(IfElseProcessor.class);
    private Predicate<ReplicationMessage> condition;
    private String ifTrueProcessorName;
    private Optional<String> ifFalseProcessorName;

    public IfElseProcessor(Predicate<ReplicationMessage> condition, String ifTrueProcessorName, Optional<String> ifFalseProcessorName) {
        this.condition = condition;
        this.ifTrueProcessorName = ifTrueProcessorName;
        this.ifFalseProcessorName = ifFalseProcessorName;
    }

    @Override
    public void process(String key, ReplicationMessage value) {
        if (value == null) {
            logger.warn("Ignoring null-message in ifelseprocessor with key: {}", key);
            return;
        }
        boolean res = condition.test(value);
        if (res) {
            context().forward(key, value, To.child(ifTrueProcessorName));
        } else {
            ifFalseProcessorName.ifPresent(e -> forwardToFalse(key, value, e));
        }
    }

    private void forwardToFalse(String key, ReplicationMessage value, String e) {
        context().forward(key, value, To.child(e));
    }

}
