/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.streams.remotejoin.ranged;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import io.floodplain.streams.api.CoreOperators;
import io.floodplain.streams.remotejoin.PreJoinProcessor;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

public class ManyToOneGroupedProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ManyToOneGroupedProcessor.class);

    private final String fromProcessorName;
    private final String withProcessorName;
    private final boolean optional;

    private final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction = (a, b) -> a.withParamMessage(b.message());
    private KeyValueStore<String, ReplicationMessage> forwardLookupStore;
    private KeyValueStore<String, ReplicationMessage> reverseLookupStore;
    private ProcessorContext<String, ReplicationMessage> context;

    public ManyToOneGroupedProcessor(String fromProcessor, String withProcessor,
                                     boolean optional) {

        this.fromProcessorName = fromProcessor;
        this.withProcessorName = withProcessor;
        this.optional = optional;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.forwardLookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(ReplicationTopologyParser.STORE_PREFIX + fromProcessorName);
        this.reverseLookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(ReplicationTopologyParser.STORE_PREFIX + withProcessorName);
    }

    @Override
    public void process(Record<String, ReplicationMessage> record) {
        String key = record.key();
        ReplicationMessage message = record.value();
        if (key.contains("{")) {
            // TODO remove, it is a bit barbaric
            throw new RuntimeException("Failed. bad key: " + key);
        }
        boolean reverse = false;
        if (key.endsWith(PreJoinProcessor.REVERSE_IDENTIFIER)) {
            reverse = true;
            key = key.substring(0, key.length() - PreJoinProcessor.REVERSE_IDENTIFIER.length());
        }
        if (reverse) {
            reverseJoin(key, message,record.timestamp());
        } else {
            forwardJoin(key, message,record.timestamp());
        }

    }

    private void reverseJoin(String key, ReplicationMessage message, long timestamp) {

        if (message == null) {
            logger.debug("reverseJoin joinGrouped emitting null message with key: {} ", key);
            context.forward(new Record<>(key,null,timestamp));
            return;
        }
        if (message.operation() == Operation.DELETE) {
            reverseJoinDelete(key, message);
            return;
        }
        final ReplicationMessage withOperation = message.withOperation(message.operation());
        try(KeyValueIterator<String, ReplicationMessage> it = forwardLookupStore.range(key + "|", key + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                String parentKey = CoreOperators.ungroupKey(keyValue.key);
                ReplicationMessage joined = joinFunction.apply(keyValue.value, withOperation);
                forwardMessage(parentKey, joined,timestamp);
            }
        }
    }

    private void reverseJoinDelete(String key, ReplicationMessage message) {
        logger.debug("Delete detected for key: {}", key);
        List<String> deleted = new ArrayList<>();
        try(KeyValueIterator<String, ReplicationMessage> it = forwardLookupStore.range(key + "|", key + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                if (optional) {
                    // Forward without joining
                    String parentKey = CoreOperators.ungroupKey(keyValue.key);
                    forwardMessage(parentKey, keyValue.value, message.timestamp());
                } else {
                    // Non optional join. Forward a delete
                    ReplicationMessage joined = joinFunction.apply(message, keyValue.value);
                    String parentKey = CoreOperators
                            .ungroupKey(keyValue.key);
                    forwardMessage(parentKey, joined.withOperation(Operation.DELETE), message.timestamp());
                    deleted.add(keyValue.key);
                }
            }
        }
        for (String deletedKey : deleted) {
            forwardLookupStore.delete(deletedKey);
        }
    }

    private void forwardJoin(String key, ReplicationMessage message, long timestamp) {
        // The forward join key is a ranged key - both the reverse and the forward key
        // are in it.
        String actualKey = CoreOperators.ungroupKey(key);
        String reverseLookupKey = CoreOperators.ungroupKeyReverse(key);
        if (message == null) {
            context.forward(new Record<>(actualKey, null,timestamp));
            return;
        }
//        if (message.operation() == Operation.DELETE) {
            // We don't need to take special action on a delete. The message has been
            // removed from the forwardStore already (in the storeProcessor),
            // and no action is needed on the joined part.
            // We do still perform the join itself before forwarding this delete. It's
            // possible a join down the line
            // requires fields from this join, so better safe than sorry.
//        }

        final ReplicationMessage withOperation = message.withOperation(message.operation());
        ReplicationMessage outerMessage = reverseLookupStore.get(reverseLookupKey);
        if (outerMessage == null) {
            // nothing found to join with, forward only if optional
            if (optional) {
                forwardMessage(actualKey, message,timestamp);
            } else {
                // I don't think this is always correct
                forwardMessage(actualKey, message.withOperation(Operation.DELETE),timestamp);
            }
        } else {
            final ReplicationMessage joined = joinFunction.apply(withOperation, outerMessage);
            forwardMessage(actualKey, joined,timestamp);
        }
    }

    private void forwardMessage(String key, ReplicationMessage innerMessage, long timestamp) {
        context.forward(new Record<>(key, innerMessage,timestamp));
        if (innerMessage.operation() == Operation.DELETE) {
            logger.debug("Delete forwarded, appending null forward with key: {}", key);
            context.forward(new Record<>(key, null,timestamp));
        }
    }

}
