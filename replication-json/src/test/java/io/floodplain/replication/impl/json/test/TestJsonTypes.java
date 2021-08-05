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
package io.floodplain.replication.impl.json.test;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TestJsonTypes {
    private static final Logger logger = LoggerFactory.getLogger(TestJsonTypes.class);
    private ReplicationMessage organization;

    @Test
    public void test() {
        System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
        ReplicationMessageParser parser = new JSONReplicationMessageParserImpl();
        ReplicationFactory.setInstance(parser);
        organization = parser.parseStream(TestJoin.class.getClassLoader().getResourceAsStream("organization.json"));
        Optional<Object> isValid = organization.value("isValid");
        Object a = isValid.get();
    }
}
