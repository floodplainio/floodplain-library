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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class FunctionProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {

    private final SetTransformer.TriFunction function;
    private ProcessorContext<String, ReplicationMessage> context;
    public FunctionProcessor(SetTransformer.TriFunction func) {
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
        if(record.value().operation()!= ReplicationMessage.Operation.DELETE) {
            ImmutableMessage applied = function.apply(record.key(), record.value().message(), record.value().paramMessage().orElse(ImmutableFactory.empty()));
            context.forward(new Record<>(record.key(), ReplicationFactory.standardMessage(applied), record.timestamp())); //.withParamMessage(value.paramMessage()); //.orElse(ImmutableFactory.empty())).withOperation(operation));

        } else {
            // Skip processing, just forward message if it is a delete
            context.forward(record);
        }
    }
}
