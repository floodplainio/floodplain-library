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
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.streams.api.CoreOperators;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HistoryProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {

    private final String lookupStoreName;
    private final String keyCounterStoreName;
    private ProcessorContext<String, ReplicationMessage> context;
    private final static Logger logger = LoggerFactory.getLogger(HistoryProcessor.class);
    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private KeyValueStore<String, Long> keyCountStore;

    public HistoryProcessor(String lookupStoreName, String keyCounterStoreName) {
        this.lookupStoreName = lookupStoreName;
        this.keyCounterStoreName = keyCounterStoreName;
    }
    @Override
    public void init(ProcessorContext<String, ReplicationMessage> context) {
        this.context = context;
        this.lookupStore = context.getStateStore(lookupStoreName);
        this.keyCountStore = context.getStateStore(keyCounterStoreName);
    }

    // TODO Maybe allow for a max size
    // TODO Also maybe max age? Otherwise data might linger forever
    @Override
    public void process(Record<String, ReplicationMessage> record) {
        String key = record.key();
        if (record.value() == null) {
            processDelete(  key);
            return;
        }
        if(record.value().operation()== ReplicationMessage.Operation.DELETE) {
            processDelete(key);
            return;
        }
        Long keyCount = keyCountStore.get(key);
        if(keyCount==null) {
            keyCount = 0L;
            keyCountStore.put(key,keyCount);
        } else {
            keyCount = keyCount.longValue() + 1;
        }

//        long recordTimestamp = record.value().timestamp();
        String formatedKey = String.format("%08d", keyCount);

        String groupedKey =key + "|" + formatedKey;
        lookupStore.put(groupedKey,record.value());
        forwardHistory(key);
    }

    // I suppose we should delete everything from this key
    // Or should deleting be part of the history?
    private void processDelete(String key) {
        // iterate over lookupStore
        List<String> toBeDeleted = new ArrayList<>();
        try(KeyValueIterator<String, ReplicationMessage> it = lookupStore.range(key + "|", key + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                toBeDeleted.add(keyValue.key);
            }
        }
        for (String k : toBeDeleted) {
            lookupStore.delete(k);
        }
        // Publish an empty list
        ReplicationMessage result = ReplicationFactory.empty().withSubMessages("list", Collections.emptyList());
        context.forward(new Record<>(key, result, result.timestamp()));

    }

    private void forwardHistory(String key) {
        logger.warn("whoop");
        List<ImmutableMessage> history = new ArrayList<>();
        try(KeyValueIterator<String, ReplicationMessage> it = lookupStore.range(key + "|", key + "}")) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                history.add(keyValue.value.message());
            }
        }
        ReplicationMessage result = ReplicationFactory.empty().withSubMessages("list",history);
        context.forward(new Record<>(key, result, result.timestamp()));
    }

}
