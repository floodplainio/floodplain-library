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
package io.floodplain.immutable.factory;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;
import io.floodplain.immutable.api.ImmutableMessageParser;
import io.floodplain.immutable.impl.ImmutableMessageImpl;
import io.floodplain.immutable.impl.JSONImmutableMessageParserImpl;
import io.floodplain.immutable.json.ImmutableJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ImmutableFactory {

    private static final ImmutableMessage empty = create(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    private final static Logger logger = LoggerFactory.getLogger(ImmutableFactory.class);

    public static ImmutableMessage empty() {
        return empty;
    }

    public static ImmutableMessage create(Map<String, ?> values, Map<String, ValueType> types) {
        return create(values, types, Collections.emptyMap(), Collections.emptyMap());
    }

    public static ImmutableMessage create(Map<String, ?> values, Map<String, ValueType> types, Map<String, ImmutableMessage> submessage, Map<String, List<ImmutableMessage>> submessages) {
        return new ImmutableMessageImpl(values, types, submessage, submessages);
    }

    public static ImmutableMessageParser createParser() {
        return new JSONImmutableMessageParserImpl();
    }

    public static String ndJson(ImmutableMessage msg) throws IOException {
        return ImmutableJSON.ndJson(msg);
    }

    public static ValueType resolveTypeFromValue(Object val) {
        if (val == null) {
            throw new NullPointerException("Can't resolve type from null value");
        } else if (val instanceof Long) {
            return ValueType.LONG;
        } else if (val instanceof Double) {
            return ValueType.DOUBLE;
        } else if (val instanceof Integer) {
            return ValueType.INTEGER;
        } else if (val instanceof Float) {
            return ValueType.FLOAT;
        } else if (val instanceof Date) {
            return ValueType.DATE;
        } else if (val instanceof Boolean) {
            return ValueType.BOOLEAN;
        } else if (val instanceof String) {
            return ValueType.STRING;
        } else if (val instanceof byte[]) {
            return ValueType.BINARY;
        } else if (val instanceof String[]) {
            return ValueType.STRINGLIST;
        } else {
            logger.warn("Unknown type::: {}", val.getClass());
            throw new IllegalArgumentException("Unknown type: " + val.getClass());
//            return ValueType.UNKNOWN;
        }
    }
}
