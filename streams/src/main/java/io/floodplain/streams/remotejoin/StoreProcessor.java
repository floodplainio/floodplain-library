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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class StoreProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {

    private final String lookupStoreName;
    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private ProcessorContext<String, ReplicationMessage> context;

    public StoreProcessor(String lookupStoreName) {
        this.lookupStoreName = lookupStoreName;
    }

    @Override
    public void init(ProcessorContext<String, ReplicationMessage> context) {
        this.context = context;
        this.lookupStore = context.getStateStore(lookupStoreName);
    }

    @Override
    public void close() {

    }

    @Override
    public void process(Record<String, ReplicationMessage> record) {
        ReplicationMessage outerMessage = record.value();
        String key = record.key();
        if (outerMessage == null || outerMessage.operation() == Operation.DELETE) {
            ReplicationMessage previous = lookupStore.get(key);
//			logger.info("Delete detected in store: {} with key: {}",lookupStoreName,key);
            if (previous != null) {
                lookupStore.delete(key);
                context.forward(record.withValue(previous.withOperation(Operation.DELETE)));
            }
            Record<String,ReplicationMessage> out = new Record<>(key, null, record.timestamp());
            context.forward(out);
        } else {
            lookupStore.put(key, outerMessage);
            context.forward(record);
        }
    }

}
