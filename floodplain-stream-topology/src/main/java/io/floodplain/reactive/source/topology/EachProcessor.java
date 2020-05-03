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
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EachProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final static Logger logger = LoggerFactory.getLogger(EachProcessor.class);
    private final ImmutableMessage.TriConsumer lambda;

    public EachProcessor(ImmutableMessage.TriConsumer lambda) {
        this.lambda = lambda;
    }


    @Override
    public void process(String key, ReplicationMessage value) {
        if(value==null || value.operation()== ReplicationMessage.Operation.DELETE) {
            // do not process delete, just forward
        } else {
            lambda.apply(key,value.message(), value.paramMessage().orElse(ImmutableFactory.empty()));
        }
        super.context().forward(key, value);
    }

}
