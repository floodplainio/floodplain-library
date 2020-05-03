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
import org.apache.kafka.streams.processor.AbstractProcessor;

public class PreJoinProcessor extends AbstractProcessor<String, ReplicationMessage> {
    public static String REVERSE_IDENTIFIER = "_REV_";


    private boolean isReverseJoin;

    public PreJoinProcessor(boolean isReverseJoin) {
        this.isReverseJoin = isReverseJoin;
    }

    @Override
    public void process(String key, ReplicationMessage msg) {
        if (isReverseJoin) {
            String newKey = key;
            newKey += REVERSE_IDENTIFIER;
            context().forward(newKey, msg == null ? null : msg.withoutParamMessage());
        } else {
            context().forward(key, msg == null ? null : msg.withoutParamMessage());
        }

    }

}
