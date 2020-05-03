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
import io.floodplain.immutable.json.ImmutableJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONImmutableMessageParserImpl implements ImmutableMessageParser {

    private static final boolean INCLUDENULLVALUES = true;

    private final static Logger logger = LoggerFactory.getLogger(JSONImmutableMessageParserImpl.class);

    @Override
    public byte[] serialize(ImmutableMessage msg) {
        return ImmutableJSON.jsonSerializer(msg, INCLUDENULLVALUES, true);
    }

    @Override
    public String describe(ImmutableMessage msg) {
        return new String(ImmutableJSON.jsonSerializer(msg, INCLUDENULLVALUES, false));
    }

    public void activate() {
        logger.info("Immutable parser constructed");
//		logger.
        ImmutableFactory.setInstance(this);
    }

    public void deactivate() {
        ImmutableFactory.setInstance(null);
    }

}
