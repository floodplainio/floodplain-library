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

import io.floodplain.immutable.api.ImmutableMessage.ValueType;

public class ImmutableTypeParser {

    public static String typeName(ValueType type) {
        switch (type) {
            case STRING:
                return "string";
            case BINARY:
                return "binary";
            case BINARY_DIGEST:
                return "binary_digest";
            case BOOLEAN:
                return "boolean";
            case DATE:
                return "date";
            case DOUBLE:
                return "double";
            case FLOAT:
                return "float";
            case INTEGER:
                return "integer";
            case STRINGLIST:
                return "stringlist";
            case LONG:
                return "long";
            case STOPWATCHTIME:
                return "stopwatchtype";
            case IMMUTABLE:
                return "immutable";
            case ENUM:
                return "enum";
            case DECIMAL:
                return "decimal";
            case CLOCKTIME:
                return "clocktime";
            case TIMESTAMP:
                return "timestamp";
            case ZONED_TIMESTAMP:
                return "zoned_timestamp";
            case LEGACYDATE:
                return "legacydate";
        }
        throw new UnsupportedOperationException("Unknown type: " + type);

    }

    public static ValueType parseType(String type) {
        switch (type) {
            case "stopwatchtime":
                return ValueType.STOPWATCHTIME;
            case "string":
                return ValueType.STRING;
            case "integer":
                return ValueType.INTEGER;
            case "long":
                return ValueType.LONG;
            case "double":
                return ValueType.DOUBLE;
            case "decimal":
                return ValueType.DECIMAL;
            case "float":
                return ValueType.FLOAT;
            case "boolean":
                return ValueType.BOOLEAN;
            case "binary_digest":
                return ValueType.BINARY_DIGEST;
            case "date":
                return ValueType.DATE;
            case "timestamp":
                return ValueType.TIMESTAMP;
            case "zoned_timestamp":
                return ValueType.ZONED_TIMESTAMP;
            case "stringlist":
                return ValueType.STRINGLIST;
            case "binary":
                return ValueType.BINARY;
            case "clocktime":
                return ValueType.CLOCKTIME;
            case "legacydate":
                return ValueType.LEGACYDATE;
            case "object":
            case "any":
            case "empty":
                return ValueType.UNKNOWN;
            case "enum":
                return ValueType.ENUM;
        }
        throw new UnsupportedOperationException("Unknown type: " + type);
    }

}
