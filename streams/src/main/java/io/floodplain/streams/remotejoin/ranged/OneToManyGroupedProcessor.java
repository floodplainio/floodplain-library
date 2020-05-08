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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class OneToManyGroupedProcessor extends AbstractProcessor<String, ReplicationMessage> {
    private static final Logger logger = LoggerFactory.getLogger(OneToManyGroupedProcessor.class);
    private final boolean debug;

    private String storeName;
    private String groupedStoreName;

    private boolean optional;

    private KeyValueStore<String, ReplicationMessage> groupedLookupStore;
    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> joinFunction;

    public OneToManyGroupedProcessor(String storeName, String groupedStoreName, boolean optional, boolean debug) {
        this.storeName = storeName;
        this.groupedStoreName = groupedStoreName;
        this.optional = optional;
        this.joinFunction = CoreOperators.getListJoinFunctionToParam(false);
        this.debug = debug;
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
        List<ReplicationMessage> msgs = new ArrayList<>();
        try (KeyValueIterator<String, ReplicationMessage> it = groupedLookupStore.range(key + "|", key + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                msgs.add(keyValue.value);
            }
        }


        ReplicationMessage joined = msg;
        if (msgs.size() > 0 || optional) {
            joined = joinFunction.apply(msg, msgs);
        }
        if (optional || msgs.size() > 0) {
            forwardMessage(key, joined);
        } else {
            // We are not optional, and have not joined with any messages. Forward a delete
            // -> TODO Improve this. It does not necesarily need a delete. If it is a new key, it can simply be ignored
            forwardMessage(key, joined.withOperation(Operation.DELETE));
        }

    }

    private void reverseJoin(String key, ReplicationMessage msg) {

        String actualKey = CoreOperators.ungroupKeyReverse(key);
        ReplicationMessage one = lookupStore.get(actualKey);
        if(debug) {
            long storeSize = lookupStore.approximateNumEntries();
            logger.info("# of elements in reverse store: {}", storeSize);
        }
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
