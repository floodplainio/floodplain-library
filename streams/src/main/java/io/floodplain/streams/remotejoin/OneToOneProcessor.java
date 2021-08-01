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
package io.floodplain.streams.remotejoin;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import io.floodplain.replication.factory.ReplicationFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class OneToOneProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {

    private final String forwardLookupStoreName;
    private final String reverseLookupStoreName;
    private KeyValueStore<String, ReplicationMessage> forwardLookupStore;
    private KeyValueStore<String, ReplicationMessage> reverseLookupStore;

    private final BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction;
    private final boolean optional;

    private static final Logger logger = LoggerFactory.getLogger(OneToOneProcessor.class);
    private ProcessorContext<String, ReplicationMessage> context;

    public OneToOneProcessor(String forwardLookupStoreName, String reverseLookupStoreName, boolean optional,
                             BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> joinFunction) {
        this.forwardLookupStoreName = forwardLookupStoreName;
        this.reverseLookupStoreName = reverseLookupStoreName;
        this.optional = optional;
        this.joinFunction = joinFunction;
    }



    @Override
    public void init(ProcessorContext<String, ReplicationMessage> context) {
        this.context = context;
        logger.info("inner lookup Looking up: " + forwardLookupStoreName);
        this.forwardLookupStore = context.getStateStore(forwardLookupStoreName);
        logger.info("inner lookup Looking up: " + reverseLookupStoreName);
        this.reverseLookupStore = context.getStateStore(reverseLookupStoreName);
        logger.info("One-to-one successfully started");
    }

    @Override
    public void process(Record<String, ReplicationMessage> record) {
        boolean reverse = false;
        String key = record.key();
        ReplicationMessage innerMessage = record.value();
        if (innerMessage == null) {
            context.forward(record.withValue(null));
            return;
        }
        KeyValueStore<String, ReplicationMessage> lookupStore = reverseLookupStore;
        if (key.endsWith(PreJoinProcessor.REVERSE_IDENTIFIER)) {
            reverse = true;
            key = key.substring(0, key.length() - PreJoinProcessor.REVERSE_IDENTIFIER.length());
            lookupStore = forwardLookupStore;
        }
        ReplicationMessage counterpart = lookupStore.get(key);
        if (counterpart == null) {
            if (reverse) {
                // We are doing a reverse join, but the original message isn't there.
                // Nothing to do for us here
            } else if (optional) {
                context.forward(new Record<>(key, innerMessage,record.timestamp()));
            }
            return;
        }
        ReplicationMessage msg;
        if (reverse) {
            if (innerMessage.operation() == Operation.DELETE && !optional) {
                // Reverse join  - the message we join with is deleted, and we are not optional
                // This means we should forward a delete too for the forward-join message
                context.forward(new Record<>(key, counterpart.withOperation(Operation.DELETE),record.timestamp()));
                context.forward(new Record<>(key, null,record.timestamp()));
            } else if (innerMessage.operation() == Operation.DELETE) {
                // The message we join with is gone, but we are optional. Forward the forward-join message as-is
                context.forward(new Record<>(key, counterpart,record.timestamp()));
            } else {
                // Regular reverse join
                msg = joinFunction.apply(counterpart, innerMessage);
                context.forward(new Record<>(key, msg,record.timestamp()));
            }
        } else {
            // Operation DELETE doesn't really matter for forward join - we can join as usual
            // The DELETE operation will be preserved and forwarded
            // TODO Shouldn't we delete from the store? I think now
            msg = joinFunction.apply(innerMessage, counterpart);
            context.forward(new Record<>(key, msg,record.timestamp()));
        }
    }

}
