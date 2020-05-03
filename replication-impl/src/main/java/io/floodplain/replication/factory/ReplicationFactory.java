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
package io.floodplain.replication.factory;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.impl.ReplicationImmutableMessageImpl;

import java.util.*;

public class ReplicationFactory {
    private static ReplicationMessageParser instance;
    private static final Runnable noopCommit = () -> {
    };


    public static ReplicationMessageParser getInstance() {
        return ReplicationFactory.instance;
    }

    public static void setInstance(ReplicationMessageParser parser) {
        ReplicationFactory.instance = parser;
    }

    public void clearInstance(ReplicationMessageParser parser) {
        ReplicationFactory.instance = parser;
    }

    public static ReplicationMessage createReplicationMessage(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, final String transactionId, final long timestamp,
                                                              final ReplicationMessage.Operation operation, final List<String> primaryKeys, Map<String, ValueType> types,
                                                              Map<String, Object> values, Map<String, ImmutableMessage> subMessageMap,
                                                              Map<String, List<ImmutableMessage>> subMessageListMap, Optional<Runnable> commitAction, Optional<ImmutableMessage> paramMessage) {
        return new ReplicationImmutableMessageImpl(source, partition, offset, transactionId, operation, timestamp, values, types, subMessageMap, subMessageListMap, primaryKeys, commitAction, paramMessage);
    }

    public static ReplicationMessage createReplicationMessage(Optional<String> source, Optional<Integer> partition, Optional<Long> offset, final String transactionId, final long timestamp,
                                                              final ReplicationMessage.Operation operation, final List<String> primaryKeys, ImmutableMessage message, Optional<Runnable> commitAction, Optional<ImmutableMessage> paramMessage) {
        return new ReplicationImmutableMessageImpl(source, partition, offset, transactionId, operation, timestamp, message, primaryKeys, commitAction, paramMessage);
    }

    public static ReplicationMessage fromMap(String key, Map<String, Object> values, Map<String, ValueType> types) {
        List<String> keys = key == null ? Collections.emptyList() : Arrays.asList(key);
        return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), null, System.currentTimeMillis(), ReplicationMessage.Operation.NONE, keys, types, values, Collections.emptyMap(), Collections.emptyMap(), Optional.of(noopCommit), Optional.empty());
    }

    public static ReplicationMessage create(Map<String, Object> values, Map<String, ValueType> types) {
        return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), null, System.currentTimeMillis(), ReplicationMessage.Operation.NONE, Collections.emptyList(), types, values, Collections.emptyMap(), Collections.emptyMap(), Optional.of(noopCommit), Optional.empty());
    }


    public static ReplicationMessage create(Map<String, Object> dataMap) {
        return new ReplicationImmutableMessageImpl(dataMap);
    }

    public static ReplicationMessage empty() {
        return ReplicationFactory.createReplicationMessage(Optional.empty(), Optional.empty(), Optional.empty(), null, System.currentTimeMillis(), ReplicationMessage.Operation.NONE, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Optional.of(noopCommit), Optional.empty());
    }

    public static ReplicationMessage joinReplicationMessage(String key, ReplicationMessage a, ReplicationMessage b) {
        return new ReplicationImmutableMessageImpl(a, b, key);
    }

    public static ReplicationMessage createErrorReplicationMessage(Throwable t) {
        return new ReplicationImmutableMessageImpl(t);
    }

    public static ReplicationMessage standardMessage(ImmutableMessage msg) {
        return new ReplicationImmutableMessageImpl(Optional.empty(), Optional.empty(), Optional.empty(), null, ReplicationMessage.Operation.NONE, -1L, msg, Collections.emptyList(), Optional.empty(), Optional.empty());
    }
}
