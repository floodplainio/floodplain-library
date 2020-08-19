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
package io.floodplain.streams.debezium;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableIdentifier {
    public final String deployment;
    public final Optional<String> tenant;
    public final Optional<String> databaseInstance;
    public final String table;
    public final ImmutableMessage keyMessage;
    public final String combinedKey;
    public final List<String> fields;


    public TableIdentifier(String tableId, ImmutableMessage keyMessage, List<String> fields) {
        final String[] primary = tableId.split("\\.");
        this.keyMessage = keyMessage;
        List<String> l = new LinkedList<>(fields);
        this.fields = Collections.unmodifiableList(l);

        if (!primary[1].contains("_")) {
            tenant = Optional.empty();
            table = primary[2];
            deployment = primary[0];
            databaseInstance = Optional.empty();
            combinedKey = fields.stream()
                    .map(field -> keyMessage.columnValue(field).toString())
                    .collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
        } else {
            deployment = primary[0];
            final String[] secondary = primary[1].split("_");
            tenant = Optional.of(secondary[0].toUpperCase());
            databaseInstance = Optional.of(secondary[1].toUpperCase());
            table = primary[2];
            combinedKey = fields.stream()
                    .map(field -> keyMessage.columnValue(field).toString())
                    .collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
        }

    }
}
