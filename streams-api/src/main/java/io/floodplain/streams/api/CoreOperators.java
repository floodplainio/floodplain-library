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
package io.floodplain.streams.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.factory.ReplicationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CoreOperators {
    private static final int TOPIC_PARTITION_COUNT = 1;
    private static final int TOPIC_REPLICATION_COUNT = 1;

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

    private static final Logger logger = LoggerFactory.getLogger(CoreOperators.class);

    private CoreOperators() {
    }

    public static int topicPartitionCount() {
        String env = System.getenv("TOPIC_PARTITION_COUNT");
        if (env != null) {
            return Integer.valueOf(env);
        }
        return TOPIC_PARTITION_COUNT;
    }

    public static int topicReplicationCount() {
        String env = System.getenv("TOPIC_REPLICATION_COUNT");
        if (env != null) {
            return Integer.valueOf(env);
        }
        return TOPIC_REPLICATION_COUNT;
    }


    public static ReplicationMessage merge(String key, ReplicationMessage a, ReplicationMessage b) {
        if (b == null) {
            return a;
        }
        return ReplicationFactory.joinReplicationMessage(key, a, b);
    }

    public static String topicName(String topicName, TopologyContext context) {
        if (topicName.contains("-generation-")) {
            logger.warn("Warning: Re-resolving topic: {}", topicName);
            Thread.dumpStack();
        }
        String topic = topicNameForReal(topicName, context);
        if (topic.indexOf('@') != -1) {
            throw new UnsupportedOperationException("Bad topic: " + topic + " from instance: " + context.instance + " tenant: " + context.tenant + " deployment: " + context.deployment + " generation: " + context.generation);
        }
        return topic;
    }

    private static String topicNameForReal(String topicName, TopologyContext context) {

        String name;
        name = topicName;


        if (name == null) {
            throw new NullPointerException("Can not create topic name when name is null. tenant: " + context.tenant.orElse("<no tenant>") + " deployment: " + context.deployment + " generation: " + context.generation);
        }

        if (name.startsWith("@")) {
            StringBuffer sb = new StringBuffer();
            if (context.tenant.isPresent()) {
                sb.append(context.tenant.get() + "-");
            }
            String[] withInstance = name.split(":");
            if (withInstance.length > 1) {
                sb.append(context.deployment + "-" + context.generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1]);
                throw new IllegalArgumentException("Instance / generational references are no longer supported");
            } else {
                sb.append(context.deployment + "-" + context.generation + "-" + context.instance + "-" + name.substring(1));
            }
            return sb.toString();
        }
        if (context.tenant.isPresent()) {
            return context.tenant.get() + "-" + context.deployment + "-" + name;
        } else {
            return context.deployment + "-" + name;
        }
    }

    public static String generationalGroup(String name, TopologyContext context) {
        if (name.startsWith("@")) {
            String[] withInstance = name.split(":");
            if (context.tenant.isPresent()) {
                if (withInstance.length > 1) {
                    return context.tenant.get() + "-" + context.deployment + "-" + context.generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                } else {
                    return context.tenant.get() + "-" + context.deployment + "-" + context.generation + "-" + context.instance + "-" + name.substring(1);
                }
            } else {
                if (withInstance.length > 1) {
                    return context.deployment + "-" + context.generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                } else {
                    return context.deployment + "-" + context.generation + "-" + context.instance + "-" + name.substring(1);
                }
            }
        }
        if (context.tenant.isPresent()) {
            return context.tenant.get() + "-" + context.deployment + "-" + context.generation + "-" + context.instance + "-" + name;
        } else {
            return context.deployment + "-" + context.generation + "-" + context.instance + "-" + name;
        }
    }


    public static BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> getParamJoinFunction() {
        return (a, b) -> a.withParamMessage(b.message());
    }

    public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> getListJoinFunctionToParam(boolean skipEmpty) {
        return (message, list) -> {
            if (list.isEmpty() && skipEmpty) {
                return message;
            } else {
                List<ImmutableMessage> withList;
                withList = list.stream().map(e -> e.message()).collect(Collectors.toList());
                return message.withParamMessage(ImmutableFactory.empty().withSubMessages("list", withList));
//                return message.withSubMessages(into, withList);
            }
        };
    }


    public static String ungroupKey(String key) {
        int index = key.lastIndexOf('|');
        if (index == -1) {
            return key;
        }
        return key.substring(index + 1, key.length());
    }

    public static String ungroupKeyReverse(String key) {
        int index = key.indexOf('|');
        if (index == -1) {
            return key;
        }
        return key.substring(0, index);
    }


}
