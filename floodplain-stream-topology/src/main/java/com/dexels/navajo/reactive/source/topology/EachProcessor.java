package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

public class EachProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final static Logger logger = LoggerFactory.getLogger(EachProcessor.class);
    private final BiConsumer<ImmutableMessage, ImmutableMessage> lambda;

    public EachProcessor(BiConsumer<ImmutableMessage,ImmutableMessage> lambda) {
        this.lambda = lambda;
    }

    @Override
    public void process(String key, ReplicationMessage value) {
        lambda.accept(value.message(),value.paramMessage().orElse(ImmutableFactory.empty()));
        super.context().forward(key, value);
    }

}
