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

    public static final String KEYSEPARATOR = "<$>";
    public static final String PRETTY_JSON = "PRETTY_JSON";
    public static final String INCLUDE_KAFKA_METADATA = "INCLUDE_KAFKA_METADATA";

    public String transactionId();

    public Optional<String> source();

    public Optional<Integer> partition();

    public Optional<Long> offset();

    public long timestamp();

    public Operation operation();

    public List<String> primaryKeys();

    public Set<String> columnNames();

    public Object columnValue(String name);

    public ValueType columnType(String name);

    public boolean equals(Object o);

    public enum Operation {
        INSERT, UPDATE, DELETE, NONE, COMMIT, MERGE, INITIAL
    }

    public String queueKey();

    public void commit();

    boolean isErrorMessage();

    public Map<String, Map<String, Object>> toDataMap();

    public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore);

    public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath);

    public Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix);
//	public Map<String, Object> flatValueMap(String prefix,Func3<String, String, Object, Object> processType);

    public boolean equalsToMessage(ReplicationMessage c);

    public boolean equalsByKey(ReplicationMessage c);

    public byte[] toBytes(ReplicationMessageParser c);

    public Map<String, ValueType> types();

    public Optional<List<ImmutableMessage>> subMessages(String field);

    public Optional<ImmutableMessage> subMessage(String field);

    public ReplicationMessage withImmutableMessage(ImmutableMessage msg);

    public ReplicationMessage withSubMessages(String field, List<ImmutableMessage> message);

    public ReplicationMessage withSubMessage(String field, ImmutableMessage message);

    public ReplicationMessage withAddedSubMessage(String field, ImmutableMessage message);

    public ReplicationMessage withoutSubMessageInList(String field, Predicate<ImmutableMessage> s);

    public ReplicationMessage withoutSubMessages(String field);

    public ReplicationMessage withoutSubMessage(String field);

    public Set<String> subMessageListNames();

    public Set<String> subMessageNames();

    public ReplicationMessage without(String columnName);

    public ReplicationMessage without(List<String> columns);

    public ReplicationMessage with(String key, Object value, ValueType type);

    public ReplicationMessage withOnlyColumns(List<String> columns);

    public ReplicationMessage withOnlySubMessages(List<String> subMessages);

    public ReplicationMessage rename(String columnName, String newName);

    public ReplicationMessage withPrimaryKeys(List<String> primary);

    public ReplicationMessage withSource(Optional<String> primary);

    public ReplicationMessage withPartition(Optional<Integer> partition);

    public ReplicationMessage withOffset(Optional<Long> offset);

    public ReplicationMessage now();

    public ReplicationMessage atTime(long timestamp);

    public String toFlatString(ReplicationMessageParser parser);

    public ReplicationMessage merge(ReplicationMessage other, Optional<List<String>> only);

    public static final boolean usePretty = true; // System.getenv(PRETTY_JSON) != null || System.getProperty(PRETTY_JSON) != null;

    public static boolean usePrettyPrint() {
        return usePretty;
    }

    //	private static final boolean includeKafkaMetadata = System.getenv(INCLUDE_KAFKA_METADATA)!=null || System.getProperty(INCLUDE_KAFKA_METADATA)!=null;
    public static boolean includeKafkaMetadata() {
        return false;
    }

    public Map<String, ImmutableMessage> subMessageMap();

    public Map<String, List<ImmutableMessage>> subMessageListMap();

    public ReplicationMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap);

    public ReplicationMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap);

    public ReplicationMessage withOperation(Operation operation);

    public Map<String, Object> values();

    public ReplicationMessage withCommitAction(Runnable commitAction);

    public ImmutableMessage message();

    public Optional<ImmutableMessage> paramMessage();

    public ReplicationMessage withParamMessage(ImmutableMessage msg);

    public ReplicationMessage withoutParamMessage();

    default public String combinedKey() {
        return primaryKeys().stream().map(k -> columnValue(k).toString()).collect(Collectors.joining(KEYSEPARATOR));
    }
}
