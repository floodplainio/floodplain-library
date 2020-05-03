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

public class ManyToManyGroupedProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private static final Logger logger = LoggerFactory.getLogger(ManyToManyGroupedProcessor.class);

    private String fromProcessorName;
    private String withProcessorName;
    private boolean optional;

    private BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> manyToManyJoinFunction;
    private KeyValueStore<String, ReplicationMessage> forwardLookupStore;
    private KeyValueStore<String, ReplicationMessage> reverseLookupStore;

    private Predicate<String, ReplicationMessage> associationBypass;

    public ManyToManyGroupedProcessor(String fromProcessor, String withProcessor,
                                      Optional<Predicate<String, ReplicationMessage>> associationBypass, boolean optional) {

        this.fromProcessorName = fromProcessor;
        this.withProcessorName = withProcessor;
        this.optional = optional;
        this.associationBypass = associationBypass.orElse((k, v) -> true);
        manyToManyJoinFunction = CoreOperators.getListJoinFunctionToParam(false);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.forwardLookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(ReplicationTopologyParser.STORE_PREFIX + fromProcessorName);
        this.reverseLookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(ReplicationTopologyParser.STORE_PREFIX + withProcessorName);
    }

    @Override
    public void process(String key, ReplicationMessage message) {
        boolean reverse = false;
        if (key.endsWith(PreJoinProcessor.REVERSE_IDENTIFIER)) {
            reverse = true;
            key = key.substring(0, key.length() - PreJoinProcessor.REVERSE_IDENTIFIER.length());
        }

        if (reverse) {
            reverseJoin(key, message);
        } else {
            forwardJoin(key, message);
        }

    }

    private void reverseJoin(String key, ReplicationMessage message) {
        // Many to many - reverse key is grouped too
        String actualKey = CoreOperators.ungroupKeyReverse(key);
        if (message == null) {
            logger.info("reverseJoin joinGrouped emitting null message with key: {} ", actualKey);
            context().forward(actualKey, null);
            return;
        }

        try (KeyValueIterator<String, ReplicationMessage> it = forwardLookupStore.range(actualKey + "|",
                actualKey + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
//				ReplicationMessage joined;

                // TODO fix this
                forwardJoin(keyValue.key, keyValue.value);

            }
        }

    }

    private void forwardJoin(String key, ReplicationMessage message) {
        // The forward join key is a ranged key - both the reverse and the forward key
        // are in it.
        String actualKey = CoreOperators.ungroupKey(key);
        String reverseLookupName = CoreOperators.ungroupKeyReverse(key);

        if (message == null) {
            logger.info("forwardJoin joinGrouped emitting null message with key: {} ", actualKey);
            context().forward(actualKey, null);
            return;
        }
        try {
            if (!associationBypass.test(actualKey, message)) {
                // filter says no, so don't join this, forward as-is
                forwardMessage(actualKey, message);
                return;
            }
        } catch (Throwable t) {
            logger.error("Error on checking filter predicate: {}", t);
        }

        if (message.operation() == Operation.DELETE) {
            // We don't need to take special action on a delete. The message has been
            // removed from the forwardStore
            // already (in the storeProcessor), and no action is needed on the joined part.
            // We do still perform the join itself before forwarding this delete. It's
            // possible a join down the line
            // requires fields from this join, so better safe than sorry.
        }

        final ReplicationMessage withOperation = message.withOperation(message.operation());

        // If we are a many to many, do a ranged lookup in the reverseLookupStore. Add
        // each match to the list, and pass it on to the join function
        List<ReplicationMessage> messageList = new ArrayList<>();
        try (KeyValueIterator<String, ReplicationMessage> it = reverseLookupStore.range(reverseLookupName + "|",
                reverseLookupName + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                messageList.add(keyValue.value);
            }
        }
        final ReplicationMessage joined;
        joined = manyToManyJoinFunction.apply(withOperation, messageList);

        if (optional || !messageList.isEmpty()) {
            forwardMessage(actualKey, joined);
        } else {
            forwardMessage(actualKey, joined.withOperation(Operation.DELETE));
        }

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
