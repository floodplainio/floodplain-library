/**
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
package io.floodplain.immutable.impl;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessageParser;
import io.floodplain.immutable.factory.ImmutableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ImmutableMessageImpl implements ImmutableMessage {

    private final static Logger logger = LoggerFactory.getLogger(ImmutableMessageImpl.class);
    private final Map<String, Object> values;
    private final Map<String, ValueType> types;

    private final Map<String, ImmutableMessage> subMessageMap;
    private final Map<String, List<ImmutableMessage>> subMessagesMap;


    public ImmutableMessageImpl(Map<String, ?> values, Map<String, ValueType> types, Map<String, ImmutableMessage> submessage, Map<String, List<ImmutableMessage>> submessages) {
        this.values = Collections.unmodifiableMap(values);
        this.types = Collections.unmodifiableMap(types);
        this.subMessageMap = Collections.unmodifiableMap(submessage);
        this.subMessagesMap = Collections.unmodifiableMap(submessages);
    }

    @Override
    public Set<String> subMessageNames() {
        if (subMessageMap == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(subMessageMap.keySet());
    }

    @Override
    public Set<String> subMessageListNames() {
        if (subMessagesMap == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(subMessagesMap.keySet());
    }

    @Override
    public Map<String, ValueType> types() {
        return this.types;
    }

    private static Function<String, Boolean> checkIgnoreList(Set<String> ignoreList) {
        return item -> !ignoreList.contains(item);
    }

    @Override
    public Map<String, Object> valueMap(boolean ignoreNull, Set<String> ignore, List<String> currentPath) {
        Map<String, Object> result = new HashMap<>();
        for (Entry<String, Object> e : values.entrySet()) {
            if (ignore.contains(e.getKey())) {
                continue;
            }
            if (e.getValue() == null && ignoreNull) {
                continue;
            }
            if (checkIgnoreList(ignore).apply(e.getKey())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        if (this.subMessageMap != null) {
            for (Entry<String, ImmutableMessage> e : subMessageMap.entrySet()) {
                List<String> withPath = new LinkedList<>(currentPath);
                withPath.add(e.getKey());
                result.put(e.getKey(), e.getValue().valueMap(ignoreNull, ignore, withPath));
            }
        }
        if (this.subMessagesMap != null) {
            for (Entry<String, List<ImmutableMessage>> e : subMessagesMap.entrySet()) {
                List<String> withPath = new LinkedList<>(currentPath);
                withPath.add(e.getKey());
                List<Map<String, Object>> elts = e.getValue().stream().map(msg -> msg.valueMap(ignoreNull, ignore, withPath)).collect(Collectors.toList());

                result.put(e.getKey(), elts);
            }
        }
        return Collections.unmodifiableMap(result);
    }

//    private Object postProcessValue(Object value) {
//        if(value instanceof LocalDate) {
//            return ((LocalDate)value).toEpochDay();
//        }
//        if(value instanceof LocalDateTime) {
//            return ((LocalDateTime)value).toInstant(ZoneOffset.UTC).toEpochMilli();
//        }
//        if (value instanceof LocalTime) {
//            return ((LocalTime)value).toSecondOfDay();
//        }
//        return value;
//    }

    @Override
    public Set<String> columnNames() {
        return this.values.keySet();
    }

    @Override
    public Optional<Object> value(String name) {
        int path = name.indexOf('/');
        if (path == -1) {
            return Optional.ofNullable(values.get(name));
        }
        String submp = name.substring(0, path);
        return subMessage(submp).orElse(ImmutableFactory.empty()).value(name.substring(path + 1));
    }

    @Override
    public ValueType columnType(String name) {
        return types.get(name);
    }

    @Override
    public String toString() {
        return "Values: " + values + " types: " + types;
    }

    @Override
    public Optional<List<ImmutableMessage>> subMessages(String field) {
        if (this.subMessagesMap == null) {
            return Optional.empty();
        }
        List<ImmutableMessage> messageList = subMessagesMap.get(field);
        if (messageList == null || messageList.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(messageList);
    }

    @Override
    public Optional<ImmutableMessage> subMessage(String field) {
        if (this.subMessageMap == null) {
            return Optional.empty();
        }
        ImmutableMessage message = subMessageMap.get(field);
        return Optional.ofNullable(message);
    }

    @Override
    public ImmutableMessage withSubMessages(String field, List<ImmutableMessage> message) {
        Map<String, List<ImmutableMessage>> res = new HashMap<>(this.subMessagesMap);
        res.put(field, message);
        return new ImmutableMessageImpl(this.values, this.types, this.subMessageMap, Collections.unmodifiableMap(res));
    }

    @Override
    public ImmutableMessage withSubMessage(String field, ImmutableMessage message) {
        Map<String, ImmutableMessage> res = new HashMap<>(this.subMessageMap);
        res.put(field, message);

        return new ImmutableMessageImpl(this.values, this.types, Collections.unmodifiableMap(res), this.subMessagesMap);
    }

    @Override
    public ImmutableMessage without(String columnName) {
        Map<String, Object> localValues = new HashMap<>(this.values);
        Map<String, ValueType> localTypes = new HashMap<>(this.types);
        localValues.remove(columnName);
        localTypes.remove(columnName);
        return new ImmutableMessageImpl(localValues, localTypes, this.subMessageMap, this.subMessagesMap);
    }

    @Override
    public ImmutableMessage without(List<String> columns) {
        Map<String, Object> localValues = new HashMap<>(this.values);
        Map<String, ValueType> localTypes = new HashMap<>(this.types);
        for (String columnName : columns) {
            localValues.remove(columnName);
            localTypes.remove(columnName);
        }
        return new ImmutableMessageImpl(localValues, localTypes, this.subMessageMap, this.subMessagesMap);
    }

    @Override
    public ImmutableMessage rename(String columnName, String newName) {
        if (columnNames().contains(columnName)) {
            Optional<Object> old = value(columnName);
            if(old.isEmpty()) {
                throw new IllegalArgumentException("Can't rename field : "+columnName+" it isn't present.");
            } else {
                ValueType renamingType = types.get(columnName);
                return this.without(columnName).with(newName, old.get(), renamingType);
            }
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ImmutableMessage with(String key, Object value, ValueType type) {

        int firstSlash = key.indexOf('/');
        if (firstSlash != -1) {
            String[] parts = key.split("/");
            Optional<ImmutableMessage> subm = subMessage(parts[0]);
            if (subm.isEmpty()) {
                logger.warn("Path: {} not found", key);
                ImmutableMessage newSub = ImmutableFactory.empty().with(key.substring(firstSlash + 1), value, type);
                return withSubMessage(parts[0], newSub);
            }
            return withSubMessage(parts[0], subm.get()).with(key.substring(firstSlash + 1), value, type);
        }
        Map<String, Object> localValues = new HashMap<>(this.values);
        Map<String, ValueType> localTypes = new HashMap<>(this.types);
        // TODO check if the key does not change?
        switch (type) {
            case IMMUTABLE:
                ImmutableMessage im = (ImmutableMessage) value;
                return this.withSubMessage(key, im);
            case IMMUTABLELIST:
                List<ImmutableMessage> iml = (List<ImmutableMessage>) value;
                return this.withSubMessages(key, iml);
            default:
                localValues.put(key, value);
                localTypes.put(key, type);
                return new ImmutableMessageImpl(localValues, localTypes, this.subMessageMap, this.subMessagesMap);

        }
    }


    @Override
    public String toFlatString(ImmutableMessageParser parser) {
        if (parser == null) {
            logger.info("Can not flatten parser, no parser present");
            return "";
        } else {
            return parser.describe(this);
        }
    }

    @Override
    public ImmutableMessage withoutSubMessages(String field) {
        Map<String, List<ImmutableMessage>> res = new HashMap<>(this.subMessagesMap);
        res.remove(field);
        return new ImmutableMessageImpl(this.values, this.types, this.subMessageMap, Collections.unmodifiableMap(res));
    }


    @Override
    public ImmutableMessage withoutSubMessage(String field) {
        Map<String, ImmutableMessage> res = new HashMap<>(this.subMessageMap);
        res.remove(field);
        return new ImmutableMessageImpl(this.values, this.types, Collections.unmodifiableMap(res), this.subMessagesMap);
    }

    @Override
    public Map<String, ImmutableMessage> subMessageMap() {
        return this.subMessageMap;
    }

    @Override
    public Map<String, List<ImmutableMessage>> subMessageListMap() {
        return this.subMessagesMap;
    }

    @Override
    public ImmutableMessage merge(ImmutableMessage other, Optional<List<String>> only) {
        ImmutableMessage msg = this;

        Map<String, ImmutableMessage> mergedSubMessageMap = new HashMap<>(this.subMessageMap);
        mergedSubMessageMap.putAll(other.subMessageMap());
        Map<String, List<ImmutableMessage>> mergedSubMessagesMap = new HashMap<>(this.subMessagesMap);
        mergedSubMessagesMap.putAll(other.subMessageListMap());

        try {
            if (only.isPresent()) {
                for (String key : only.get()) {

                    ImmutableMessage lookupMsg = other;
                    while (key.contains(".") && lookupMsg != null) {

                        String submsgName = key.substring(0, key.indexOf('.'));
                        key = key.substring(submsgName.length() + 1);
                        Optional<ImmutableMessage> subMessage = lookupMsg.subMessage(submsgName);
                        if (subMessage.isPresent()) {
                            if (lookupMsg == other) mergedSubMessageMap.remove(submsgName);
                            lookupMsg = subMessage.get();
                        } else {
                            lookupMsg = null;
                        }
                    }

                    Optional<Object> found;
                    if (lookupMsg != null) {
                        found = lookupMsg.value(key);
                        if (found.isPresent()) {
                            msg = msg.with(key, found.get(), lookupMsg.columnType(key));
                        } else {
                            Optional<ImmutableMessage> subMessage = lookupMsg.subMessage(key);
                            if (subMessage.isPresent()) {
                                mergedSubMessageMap.put(key, subMessage.get());
                            }

                            Optional<List<ImmutableMessage>> subMessages = lookupMsg.subMessages(key);
                            if (subMessages.isPresent()) {
                                mergedSubMessagesMap.put(key, subMessages.get());
                            }
                        }
                    }
                }
            } else {
                for (String key : other.columnNames()) {
                    Optional<Object> found = other.value(key);
                    if (found.isPresent()) {
                        msg = msg.with(key, found.get(), other.columnType(key));
                    }
                }

            }

        } catch (Throwable t) {
            logger.error("Err", t);

        }


        return msg.withAllSubMessageLists(mergedSubMessagesMap).withAllSubMessage(mergedSubMessageMap);
    }

    public ImmutableMessage withAllSubMessageLists(Map<String, List<ImmutableMessage>> subMessageListMap) {
        return new ImmutableMessageImpl(values, types, this.subMessageMap, subMessageListMap);
    }

    public ImmutableMessage withAllSubMessage(Map<String, ImmutableMessage> subMessageMap) {
        return new ImmutableMessageImpl(values, types, subMessageMap, subMessagesMap);
    }

    @Override
    public ImmutableMessage withAddedSubMessage(String field, ImmutableMessage message) {
        List<ImmutableMessage> subMessageList = new ArrayList<>(subMessages(field).orElse(new ArrayList<>()));
        subMessageList.add(message);
        return withSubMessages(field, subMessageList);
    }

    @Override
    public ImmutableMessage withoutSubMessageInList(String field, Predicate<ImmutableMessage> selector) {
        List<ImmutableMessage> subMessageList = subMessages(field).orElse(Collections.emptyList())
                .stream()
                .filter(m -> !selector.test(m))
                .collect(Collectors.toList());
        return withSubMessages(field, subMessageList);
    }

    @Override
    public Map<String, Object> flatValueMap(boolean ignoreNull, Set<String> ignore, String prefix) {
        Map<String, Object> localValues;
        if ("".equals(prefix)) {
            localValues = new HashMap<>(this.values);
        } else {
            localValues = new HashMap<>();
            for (Entry<String, Object> e : this.values.entrySet()) {
                localValues.put(prefix + "/" + e.getKey(), e.getValue());
            }
        }
        for (Entry<String, ImmutableMessage> e : this.subMessageMap.entrySet()) {
            String newPrefix = !"".equals(prefix) ? prefix + "_" + e.getKey() : e.getKey();
            localValues.putAll(e.getValue().flatValueMap(ignoreNull, ignore, newPrefix));
        }
        if (!subMessagesMap.isEmpty()) {
            for (Entry<String, List<ImmutableMessage>> e : this.subMessagesMap.entrySet()) {
                int i = 0;
                for (ImmutableMessage msg : e.getValue()) {
                    String pr = e.getKey() + "@" + i;
                    String newPrefix = !"".equals(prefix) ? prefix + "/" + pr : pr;
                    msg.flatValueMap(ignoreNull, ignore, newPrefix).forEach(localValues::put);
                    i++;
                }
            }
        }
        return Collections.unmodifiableMap(localValues);
    }


    @Override
    public Map<String, Object> flatValueMap(String prefix, Trifunction processType) {
        Map<String, Object> localValues = getFlatValueMap(prefix, processType);
        for (Entry<String, ImmutableMessage> e : this.subMessageMap.entrySet()) {
            String newPrefix = !"".equals(prefix) ? prefix + "_" + e.getKey() : e.getKey();
            localValues.putAll(e.getValue().flatValueMap(newPrefix, processType));
        }
        return Collections.unmodifiableMap(localValues);
    }

    private Map<String, Object> getFlatValueMap(String prefix, Trifunction processType) {
        Map<String, Object> localValues;
        if ("".equals(prefix)) {
            localValues = new HashMap<>();
            for (Entry<String, Object> e : this.values.entrySet()) {
                ValueType type = this.types.get(e.getKey());
                final Object processed = processType.apply(e.getKey(), type, e.getValue());
                if (processed != null) {
                    localValues.put(e.getKey(), processed);
                }
            }
        } else {
            localValues = new HashMap<>();
            for (Entry<String, Object> e : this.values.entrySet()) {
                ValueType type = this.types.get(e.getKey());
                final Object processed = processType.apply(e.getKey(), type, e.getValue());
                if (processed != null) {
                    localValues.put(prefix + "_" + e.getKey(), processed);
                }
            }
        }
        return localValues;
    }

    @Override
    public Map<String, Object> values() {
        return this.values;
    }

    @Override
    public Map<String, TypedData> toTypedDataMap() {
        Map<String, TypedData> columns = new HashMap<>();
        this.values.forEach((key,value) -> columns.put(key, new TypedData(this.columnType(key), value)));
        return columns;
    }


}
