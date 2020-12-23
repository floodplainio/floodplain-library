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
package io.floodplain.streams.debezium.impl;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.floodplain.streams.debezium.JSONToReplicationMessage;
import io.floodplain.streams.debezium.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Optional;

public class DebeziumConversionProcessor implements Processor<String, byte[]> {

    private ProcessorContext processorContext;

    public DebeziumConversionProcessor() {
    }

    @Override
    public void close() {

    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;

    }

    @Override
    public void process(String key, byte[] value) {
        if (value == null) {
            return;
        }
        KeyValue keyValue = JSONToReplicationMessage.parse(key, value);
        FallbackReplicationMessageParser ftm = new FallbackReplicationMessageParser(true);
        ReplicationMessage msg = ftm.parseBytes(Optional.empty(), keyValue.value);
        processorContext.forward(keyValue.key, msg);
    }

}
