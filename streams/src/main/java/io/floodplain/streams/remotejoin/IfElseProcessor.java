/**
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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

public class IfElseProcessor extends AbstractProcessor<String, ReplicationMessage> {
    private final static Logger logger = LoggerFactory.getLogger(IfElseProcessor.class);
    private final Predicate<ReplicationMessage> condition;
    private final String ifTrueProcessorName;
    private final Optional<String> ifFalseProcessorName;

    public IfElseProcessor(Predicate<ReplicationMessage> condition, String ifTrueProcessorName, Optional<String> ifFalseProcessorName) {
        this.condition = condition;
        this.ifTrueProcessorName = ifTrueProcessorName;
        this.ifFalseProcessorName = ifFalseProcessorName;
    }

    @Override
    public void process(String key, ReplicationMessage value) {
        if (value == null) {
            logger.warn("Ignoring null-message in ifelseprocessor with key: {}", key);
            return;
        }
        boolean res = condition.test(value);
        if (res) {
            context().forward(key, value, To.child(ifTrueProcessorName));
        } else {
            ifFalseProcessorName.ifPresent(e -> forwardToFalse(key, value, e));
        }
    }

    private void forwardToFalse(String key, ReplicationMessage value, String e) {
        context().forward(key, value.withOperation(ReplicationMessage.Operation.NONE), To.child(e));
    }

}
