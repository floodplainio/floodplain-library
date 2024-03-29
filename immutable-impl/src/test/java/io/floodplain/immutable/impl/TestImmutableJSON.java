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
package io.floodplain.immutable.impl;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessageParser;
import io.floodplain.immutable.factory.ImmutableFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Optional;

import static io.floodplain.immutable.api.ImmutableMessage.ValueType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestImmutableJSON {

    private ImmutableMessageParser parser;

    private final static Logger logger = LoggerFactory.getLogger(TestImmutableJSON.class);

    @BeforeEach
    public void setup() {
        parser = new JSONImmutableMessageParserImpl();
    }

    @Test
    public void testImmutable() {
        ImmutableMessage msg = ImmutableFactory.empty().with("teststring", "bla", STRING)
                .with("testinteger", 3, INTEGER);
        byte[] bytes = parser.serialize(msg);
        logger.info("TEST: {}", new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testDescribe() {
        ImmutableMessage msg = ImmutableFactory.empty().with("teststring", "bla", STRING)
                .with("testinteger", 3, INTEGER);
        String description = parser.describe(msg);
        logger.info("DESCRIPTION: {}", description);
    }

    @Test
    public void testAddSubMessage() {
        ImmutableMessage empty = ImmutableFactory.empty();
        ImmutableMessage created = empty.with("Aap/Noot", 3, INTEGER);
        Optional<ImmutableMessage> sub = created.subMessage("Aap");
        assertTrue(sub.isPresent());
        assertEquals(3, sub.get().value("Noot").orElse(-1));
    }

    @Test
    public void testGetSubValue() {
        ImmutableMessage empty = ImmutableFactory.empty();
        ImmutableMessage created = empty.with("Aap/Noot", 3, INTEGER);
        assertEquals(3, created.value("Aap/Noot").orElse(-1));
    }

    @Test
    public void testSubMessageUsingWith() {
        ImmutableMessage created = ImmutableFactory.empty().with("Aap", 3, INTEGER);
        ImmutableMessage someOther = ImmutableFactory.empty().with("Noot", 4, INTEGER);
        ImmutableMessage combined = created.with("submessage", someOther, IMMUTABLE);
        assertEquals(4, combined.value("submessage/Noot").orElse(-1));
    }

    @Test
    public void testDate() {
        LocalDate d = LocalDate.now();
        ImmutableMessage created = ImmutableFactory.empty().with("Date", d, DATE);
        Object result = created.value("Date").get();
        assertEquals(d,result);
        assertEquals(DATE,created.columnType("Date"));
    }


    @Test
    public void testNdJSON() throws IOException {
        ImmutableMessage m = ImmutableFactory.empty().with("somenumber", 3, INTEGER);
        logger.info("{}", ImmutableFactory.ndJson(m));

    }
}
