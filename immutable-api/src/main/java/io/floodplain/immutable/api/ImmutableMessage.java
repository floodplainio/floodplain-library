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
package io.floodplain.immutable.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;


public interface ImmutableMessage {

    enum ValueType {
        STRING,
        INTEGER,
        LONG,
        DOUBLE,
        FLOAT,
        BOOLEAN,
        BINARY_DIGEST,
        DATE,
        LIST,
        BINARY,
        COORDINATE,
        CLOCKTIME,
        STOPWATCHTIME,
        IMMUTABLE,
        UNKNOWN,
        IMMUTABLELIST,
        POINT,
        REACTIVE,
        REACTIVESCRIPT,
        REACTIVEPIPE,
        REACTIVEPARTIALPIPE,
        MAPPER,
        ENUM,
        STRINGLIST;
    }

    public Set<String> columnNames();

    /**
     * Use value(name) instead
     *
     * @param name name of column
     * @return
     */
    @Deprecated
    public Object columnValue(String name);

    public ValueType columnType(String name);

    public byte[] toBytes(ImmutableMessageParser c);

    default public Optional<Object> value(String name) {
        return Optional.ofNullable(columnValue(name));
    }

    public Map<String, ValueType> types();

    public Set<String> subMessageListNames();

    public Set<String> subMessageNames();

    public Map<String, Object> values();

    public Map<String, TypedData> toTypedDataMap();

    public Map<String, Map<String, Object>> toDataMap();

    public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore);

    public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath);

    public Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix);

    public ImmutableMessage merge(ImmutableMessage other, Optional<List<String>> only);

    public String toFlatString(ImmutableMessageParser parser);

    public Optional<List<ImmutableMessage>> subMessages(String field);

    public Optional<ImmutableMessage> subMessage(String field);

    public Map<String, ImmutableMessage> subMessageMap();

    public Map<String, List<ImmutableMessage>> subMessageListMap();

    public ImmutableMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap);

    public ImmutableMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap);

    public ImmutableMessage withSubMessages(String field, List<ImmutableMessage> message);

    public ImmutableMessage withSubMessage(String field, ImmutableMessage message);

    public ImmutableMessage withAddedSubMessage(String field, ImmutableMessage message);

    public ImmutableMessage withoutSubMessageInList(String field, Predicate<ImmutableMessage> s);

    public ImmutableMessage withoutSubMessages(String field);

    public ImmutableMessage withoutSubMessage(String field);

    public ImmutableMessage without(String columnName);

    public ImmutableMessage without(List<String> columns);

    public ImmutableMessage with(String key, Object value, ValueType type);

    public ImmutableMessage withOnlyColumns(List<String> columns);

    public ImmutableMessage withOnlySubMessages(List<String> subMessages);

    public ImmutableMessage rename(String columnName, String newName);

    Map<String, Object> flatValueMap(String prefix, Trifunction processType);

    public static interface Trifunction {
        Object apply(String key, ValueType type, Object value);
    }

    public static interface TriConsumer {
        public void apply(String key,ImmutableMessage message, ImmutableMessage secondary);
    }
    public class TypedData {
        public final ValueType type;
        public final Object value;

        public TypedData(ValueType type, Object value) {
            this.type = type;
            this.value = value;
        }
    }

}
