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
package io.floodplain.streams.testdata;

import io.floodplain.streams.api.CoreOperators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestUngroupKey {

    private final static Logger logger = LoggerFactory.getLogger(TestUngroupKey.class);


    @Test
    public void test() {
        String key = "aap|noot";
        final String ungrouped = CoreOperators.ungroupKey(key);
        Assertions.assertEquals("noot", ungrouped);
        logger.info(ungrouped);
    }

    @Test
    public void testTriple() {
        String key = "aap|noot|mies";
        final String ungrouped = CoreOperators.ungroupKey(key);
        Assertions.assertEquals("mies", ungrouped);
        logger.info(ungrouped);
    }

}
