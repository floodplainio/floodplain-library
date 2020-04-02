package com.dexels.kafka.streams.remotejoin.ranged;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.remotejoin.PreJoinProcessor;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public class OneToManyGroupedProcessor extends AbstractProcessor<String, ReplicationMessage> {
    private static final Logger logger = LoggerFactory.getLogger(OneToManyGroupedProcessor.class);

    private String storeName;
    private String groupedStoreName;

    private boolean optional;

    private KeyValueStore<String, ReplicationMessage> groupedLookupStore;
    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> joinFunction;

    private Predicate<String, ReplicationMessage> filterPredicate;

    public OneToManyGroupedProcessor(String storeName, String groupedStoreName, boolean optional,
                                     Optional<Predicate<String, ReplicationMessage>> filterPredicate,
                                     BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> joinFunction) {
        this.storeName = storeName;
        this.groupedStoreName = groupedStoreName;
        this.optional = optional;
        this.joinFunction = joinFunction;
        this.filterPredicate = filterPredicate.orElse((k, v) -> true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(storeName);
        this.groupedLookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(groupedStoreName);
        super.init(context);
    }

    @Override
    public void process(String key, ReplicationMessage msg) {
        boolean reverse = false;
        if (key.endsWith(PreJoinProcessor.REVERSE_IDENTIFIER)) {
            reverse = true;
            key = key.substring(0, key.length() - PreJoinProcessor.REVERSE_IDENTIFIER.length());
        }

        if (reverse) {
            reverseJoin(key, msg);
        } else {
            if (msg == null) {
                logger.debug("O2M Emitting null message with key: {}", key);
                context().forward(key, null);
                return;
            }
            forwardJoin(key, msg);
        }

    }

    private void forwardJoin(String key, ReplicationMessage msg) {

        try {
            if (!filterPredicate.test(key, msg)) {
                // filter says no
                forwardMessage(key, msg.withOperation(Operation.DELETE));
                return;
            }
        } catch (Throwable t) {
            logger.error("Error on checking filter predicate: {}", t);
        }

        List<ReplicationMessage> msgs = new ArrayList<>();
        try (KeyValueIterator<String, ReplicationMessage> it = groupedLookupStore.range(key + "|", key + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                msgs.add(keyValue.value);
            }
        }


        ReplicationMessage joined = msg;
        if (msgs.size() > 0) {
            joined = joinFunction.apply(msg, msgs);
        }
        if (optional || msgs.size() > 0) {
            forwardMessage(key, joined);
        } else {
            // We are not optional, and have not joined with any messages. Forward a delete
            forwardMessage(key, joined.withOperation(Operation.DELETE));
        }

    }

    private void reverseJoin(String key, ReplicationMessage msg) {

        String actualKey = CoreOperators.ungroupKeyReverse(key);
        ReplicationMessage one = lookupStore.get(actualKey);
        if (one == null) {
            // We are doing a reverse join, but the original message isn't there.
            // Nothing to do for us here
            return;
        }
        // OneToMany, thus we need to find all the other messages that
        // we also have to join with us. Effectively the same as a
        // forward join.
        forwardJoin(actualKey, one);
    }

    private void forwardMessage(String key, ReplicationMessage innerMessage) {
        context().forward(key, innerMessage);
        // flush downstream stores with null:
        if (innerMessage.operation() == Operation.DELETE) {
            logger.debug("Delete forwarded, appending null forward with key: {}", key);
            context().forward(key, null);
        }
    }


}
