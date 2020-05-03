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
package io.floodplain.replication.api;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface ReplicationMessage {

    String KEYSEPARATOR = "<$>";
    String PRETTY_JSON = "PRETTY_JSON";

    String transactionId();

    Optional<String> source();

    Optional<Integer> partition();

    Optional<Long> offset();

    long timestamp();

    Operation operation();

    List<String> primaryKeys();

    Set<String> columnNames();

    Object columnValue(String name);

    ValueType columnType(String name);

    boolean equals(Object o);

    enum Operation {
        INSERT, UPDATE, DELETE, NONE, COMMIT, MERGE, INITIAL
    }

    String queueKey();

    void commit();

    boolean isErrorMessage();

    Map<String, Map<String, Object>> toDataMap();

    Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore);

    Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath);

    Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix);
//	Map<String, Object> flatValueMap(String prefix,Func3<String, String, Object, Object> processType);

    boolean equalsToMessage(ReplicationMessage c);

    boolean equalsByKey(ReplicationMessage c);

    byte[] toBytes(ReplicationMessageParser c);

    Map<String, ValueType> types();

    Optional<List<ImmutableMessage>> subMessages(String field);

    Optional<ImmutableMessage> subMessage(String field);

    ReplicationMessage withImmutableMessage(ImmutableMessage msg);

    ReplicationMessage withSubMessages(String field, List<ImmutableMessage> message);

    ReplicationMessage withSubMessage(String field, ImmutableMessage message);

    ReplicationMessage withAddedSubMessage(String field, ImmutableMessage message);

    ReplicationMessage withoutSubMessageInList(String field, Predicate<ImmutableMessage> s);

    ReplicationMessage withoutSubMessages(String field);

    ReplicationMessage withoutSubMessage(String field);

    Set<String> subMessageListNames();

    Set<String> subMessageNames();

    ReplicationMessage without(String columnName);

    ReplicationMessage without(List<String> columns);

    ReplicationMessage with(String key, Object value, ValueType type);

    ReplicationMessage withOnlyColumns(List<String> columns);

    ReplicationMessage withOnlySubMessages(List<String> subMessages);

    ReplicationMessage rename(String columnName, String newName);

    ReplicationMessage withPrimaryKeys(List<String> primary);

    ReplicationMessage withSource(Optional<String> primary);

    ReplicationMessage withPartition(Optional<Integer> partition);

    ReplicationMessage withOffset(Optional<Long> offset);

    ReplicationMessage now();

    ReplicationMessage atTime(long timestamp);

    String toFlatString(ReplicationMessageParser parser);

    ReplicationMessage merge(ReplicationMessage other, Optional<List<String>> only);

    boolean usePretty =  System.getenv(PRETTY_JSON) != null || System.getProperty(PRETTY_JSON) != null;

    static boolean usePrettyPrint() {
        return usePretty;
    }

    //	private static final boolean includeKafkaMetadata = System.getenv(INCLUDE_KAFKA_METADATA)!=null || System.getProperty(INCLUDE_KAFKA_METADATA)!=null;
    static boolean includeKafkaMetadata() {
        return false;
    }

    Map<String, ImmutableMessage> subMessageMap();

    Map<String, List<ImmutableMessage>> subMessageListMap();

    ReplicationMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap);

    ReplicationMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap);

    ReplicationMessage withOperation(Operation operation);

    Map<String, Object> values();

    ReplicationMessage withCommitAction(Runnable commitAction);

    ImmutableMessage message();

    Optional<ImmutableMessage> paramMessage();

    ReplicationMessage withParamMessage(ImmutableMessage msg);

    ReplicationMessage withoutParamMessage();

    default String combinedKey() {
        return primaryKeys().stream().map(k -> columnValue(k).toString()).collect(Collectors.joining(KEYSEPARATOR));
    }
}
