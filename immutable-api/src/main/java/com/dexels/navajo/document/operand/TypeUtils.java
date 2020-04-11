package com.dexels.navajo.document.operand;

import com.dexels.immutable.api.ImmutableMessage;

import java.util.List;

import static com.dexels.immutable.api.ImmutableMessage.ValueType;

public class TypeUtils {

    public static final ValueType determineNavajoType(Object o) {
        return determineNavajoType(o, ValueType.UNKNOWN);
    }

    public static final ValueType determineNavajoType(Object o, ValueType defaultType) {

        if (o == null) {
            return defaultType;
        }
        if (o instanceof Integer)
            return ValueType.INTEGER;
        else if (o instanceof String)
            return ValueType.STRING;
        else if (o instanceof java.util.Date)
            return ValueType.DATE;
        else if (o instanceof Double)
            return ValueType.DOUBLE;
        else if (o instanceof Float)
            return ValueType.FLOAT;
        else if (o instanceof List)
            return ValueType.LIST;
        else if (o instanceof String[])
            return ValueType.STRINGLIST;
        else if (o instanceof Boolean)
            return ValueType.BOOLEAN;
        else if (o instanceof ClockTime)
            return ValueType.CLOCKTIME;
        else if (o instanceof StopwatchTime)
            return ValueType.STOPWATCHTIME;
            // Added by frank... To enable tipi-expressions, without creating a dep
//        else if (o instanceof Binary)
//            return ValueType.BINARY;
        else if (o instanceof Coordinate) {
            return ValueType.COORDINATE;
        } else if (o instanceof ImmutableMessage) {
            return ValueType.IMMUTABLE;
        }
        return defaultType;
    }

}
