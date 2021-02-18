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
import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;

import java.util.function.BiFunction;

public class FilterProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final BiFunction<String,ImmutableMessage, Boolean> filterExpression;

    public FilterProcessor(BiFunction<String,ImmutableMessage, Boolean> func) {
        this.filterExpression = func;
    }

    @Override
    public void process(String key, ReplicationMessage value) {
        if(value==null) {
            super.context().forward(key, null);
            return;
        }
        if (filterExpression.apply(key,value.message())) {
            super.context().forward(key, value);
        }
    }

}
