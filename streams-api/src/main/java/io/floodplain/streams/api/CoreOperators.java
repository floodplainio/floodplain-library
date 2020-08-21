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

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class CoreOperators {
    private static final int TOPIC_PARTITION_COUNT = 1;
    private static final int TOPIC_REPLICATION_COUNT = 1;

    private CoreOperators() {
    }

    public static int topicReplicationCount() {
        String env = System.getenv("TOPIC_REPLICATION_COUNT");
        if (env != null) {
            return Integer.parseInt(env);
        }
        return TOPIC_REPLICATION_COUNT;
    }




    public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> getListJoinFunctionToParam(boolean skipEmpty) {
        return (message, list) -> {
            if (list.isEmpty() && skipEmpty) {
                return message;
            } else {
                List<ImmutableMessage> withList;
                withList = list.stream().map(ReplicationMessage::message).collect(Collectors.toList());
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
        return key.substring(index + 1);
    }

    public static String ungroupKeyReverse(String key) {
        int index = key.indexOf('|');
        if (index == -1) {
            return key;
        }
        return key.substring(0, index);
    }


}
