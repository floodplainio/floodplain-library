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

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import io.floodplain.replication.factory.ReplicationFactory;
import kotlin.jvm.functions.Function3;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;


public class CompareToProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {

    private final String lookupStoreName;
    private final Function3<String, ImmutableMessage, ImmutableMessage, ImmutableMessage> transform;
    private KeyValueStore<String, ReplicationMessage> lookupStore;

    private final static Logger logger = LoggerFactory.getLogger(CompareToProcessor.class);
    private ProcessorContext<String,ReplicationMessage> context;


    public CompareToProcessor(String lookupStoreName, Function3<String, ImmutableMessage, ImmutableMessage, ImmutableMessage> transform) {
        this.lookupStoreName = lookupStoreName;
        this.transform = transform;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(lookupStoreName);
    }

    @Override
    public void process(Record<String, ReplicationMessage> record) {
        ReplicationMessage incoming = record.value();
        String key = record.key();
        if (incoming == null || incoming.operation() == Operation.DELETE) {
            logger.debug("Delete detected in store: {} with key: {}", lookupStoreName, key);
            ReplicationMessage previous = lookupStore.get(key);
            if (previous != null) {
                lookupStore.delete(key);
                ReplicationMessage forwarding = previous
                        .withOperation(Operation.DELETE);
                context.forward(record.withValue(forwarding));
            }
        } else {
            ReplicationMessage previous = lookupStore.get(key);
            ImmutableMessage result = null;
            if (previous != null) {
//                ReplicationMessage forwarding = record.value().withParamMessage(previous.message());
                result = transform.invoke(key,incoming.message(),previous.message());
//                context.forward(record.withValue(ReplicationFactory.standardMessage(result)));
            } else {
                // 'new message'
                result = transform.invoke(key,incoming.message(),null);
//                context.forward(record.withValue(ReplicationFactory.standardMessage(result)));
            }
            context.forward(record.withValue(ReplicationFactory.standardMessage(result)));

            lookupStore.put(key, ReplicationFactory.standardMessage(result));
        }
    }

    @Override
    public void close() {
    }

}
