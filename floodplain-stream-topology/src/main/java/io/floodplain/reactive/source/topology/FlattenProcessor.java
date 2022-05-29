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
package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import kotlin.Pair;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.List;

public class FlattenProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {


    private final FlattenTransformer.TriFunction function;
    private ProcessorContext<String, ReplicationMessage> context;
    public FlattenProcessor(FlattenTransformer.TriFunction func) {
        this.function = func;
    }

    @Override
    public void init(ProcessorContext<String, ReplicationMessage> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, ReplicationMessage> record) {
        if (record.value() == null) {
            // forward nulls unchanged
            context.forward(record);
            return;
        }
        ReplicationMessage.Operation op = record.value().operation();
        List<Pair<String,ImmutableMessage>> applied = function.apply(record.key(), record.value().message(), record.value().paramMessage().orElse(ImmutableFactory.empty()));
        for(Pair<String,ImmutableMessage> item: applied) {
            context.forward(new Record<>(item.component1(), ReplicationFactory.standardMessage(item.component2()).withOperation(op), record.timestamp())); //.withParamMessage(value.paramMessage()); //.orElse(ImmutableFactory.empty())).withOperation(operation));
        }
    }
}
