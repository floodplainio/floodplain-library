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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RowNumberProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final String lookupStoreName;
    private KeyValueStore<String, ReplicationMessage> lookupStore;

    private final static Logger logger = LoggerFactory.getLogger(RowNumberProcessor.class);


    public RowNumberProcessor(String lookupStoreName) {
        this.lookupStoreName = lookupStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(lookupStoreName);
        super.init(context);
    }

    @Override
    public void close() {

    }


    @Override
    public void process(String key, ReplicationMessage incoming) {
        if (incoming == null) {
            logger.warn("RowNumber processor does not support deletes yet");
            return;
        }
        ReplicationMessage existing = this.lookupStore.get(key);
        long row;
        if (existing == null) {
            // 1 based
            row = this.lookupStore.approximateNumEntries() + 1;
            this.lookupStore.put(key, ReplicationFactory.empty().with("row", row, ImmutableMessage.ValueType.LONG));
        } else {
            row = (long) existing.columnValue("row");
        }
        context().forward(key, incoming.with("_row", row, ImmutableMessage.ValueType.LONG));
    }

}
