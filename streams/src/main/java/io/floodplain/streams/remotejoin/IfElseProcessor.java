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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

public class IfElseProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {
    private final static Logger logger = LoggerFactory.getLogger(IfElseProcessor.class);
    private final Predicate<ReplicationMessage> condition;
    private final String ifTrueProcessorName;
    private final Optional<String> ifFalseProcessorName;
    private ProcessorContext<String,ReplicationMessage> context;

    public IfElseProcessor(Predicate<ReplicationMessage> condition, String ifTrueProcessorName, Optional<String> ifFalseProcessorName) {
        this.condition = condition;
        this.ifTrueProcessorName = ifTrueProcessorName;
        this.ifFalseProcessorName = ifFalseProcessorName;
    }

    private void forwardToFalse(String key, ReplicationMessage value, long timestamp, String e) {
        context.forward(new Record<> (key, value.withOperation(ReplicationMessage.Operation.NONE),timestamp), e);
    }

    @Override
    public void process(Record<String,ReplicationMessage> record) {
        ReplicationMessage value = record.value();
        String key = record.key();
        if (value == null) {
            logger.warn("Ignoring null-message in ifelseprocessor with key: {}", key);
            return;
        }
        boolean res = condition.test(value);
        if (res) {
            context.forward(new Record<>(key,value,record.timestamp()), ifTrueProcessorName);
        } else {
            ifFalseProcessorName.ifPresent(e -> forwardToFalse(key, value,record.timestamp(), e));
        }
    }

    @Override
    public void init(ProcessorContext<String,ReplicationMessage> context) {
        this.context = context;
    }
}
