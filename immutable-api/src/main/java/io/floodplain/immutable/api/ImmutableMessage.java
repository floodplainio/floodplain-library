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
        DECIMAL,
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
        STRINGLIST
    }

    Set<String> columnNames();

    /**
     * Use value(name) instead
     *
     * @param name name of column
     * @return
     */
    @Deprecated
    Object columnValue(String name);

    ValueType columnType(String name);

    default Optional<Object> value(String name) {
        return Optional.ofNullable(columnValue(name));
    }

    Map<String, ValueType> types();

    Set<String> subMessageListNames();

    Set<String> subMessageNames();

    Map<String, Object> values();

    Map<String, TypedData> toTypedDataMap();

    Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath);

    Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix);

    ImmutableMessage merge(ImmutableMessage other, Optional<List<String>> only);

    String toFlatString(ImmutableMessageParser parser);

    Optional<List<ImmutableMessage>> subMessages(String field);

    Optional<ImmutableMessage> subMessage(String field);

    Map<String, ImmutableMessage> subMessageMap();

    Map<String, List<ImmutableMessage>> subMessageListMap();

    ImmutableMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap);

    ImmutableMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap);

    ImmutableMessage withSubMessages(String field, List<ImmutableMessage> message);

    ImmutableMessage withSubMessage(String field, ImmutableMessage message);

    ImmutableMessage withAddedSubMessage(String field, ImmutableMessage message);

    ImmutableMessage withoutSubMessageInList(String field, Predicate<ImmutableMessage> s);

    ImmutableMessage withoutSubMessages(String field);

    ImmutableMessage withoutSubMessage(String field);

    ImmutableMessage without(String columnName);

    ImmutableMessage without(List<String> columns);

    ImmutableMessage with(String key, Object value, ValueType type);

    ImmutableMessage rename(String columnName, String newName);

    Map<String, Object> flatValueMap(String prefix, Trifunction processType);

    interface Trifunction {
        Object apply(String key, ValueType type, Object value);
    }

    interface TriConsumer {
        void apply(String key, ImmutableMessage message, ImmutableMessage secondary);
    }
    class TypedData {
        public final ValueType type;
        public final Object value;

        public TypedData(ValueType type, Object value) {
            this.type = type;
            this.value = value;
        }
    }

}
