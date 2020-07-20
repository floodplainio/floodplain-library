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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LogProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final static Logger logger = LoggerFactory.getLogger(LogProcessor.class);

    @Override
    public void process(String key, ReplicationMessage value) {
        logger.info("TTPP>> {} key serde: {} value serde: {}",key,context().keySerde(),context().valueSerde());
//        super.context().
//        Serializer<ReplicationMessage> sz = (Serializer<ReplicationMessage>) super.context().valueSerde().serializer();
//        String serializedForm = new String(sz.serialize(this.context().topic(),value));
//        logger.info("Serialized: {}",serializedForm);
////serializer                .serialize(value);
        super.context().forward(key, value);
    }

}