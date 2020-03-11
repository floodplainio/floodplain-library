package com.dexels.immutable.api;

import com.dexels.immutable.api.ImmutableMessage.ValueType;

public class ImmutableTypeParser {
	
	public static String typeName(ValueType type) {
		switch (type) {
		case STRING:
			return "string";
		case BINARY:
			return "binary";
		case BINARY_DIGEST:
			return "binary";
		case BOOLEAN:
			return "boolean";
		case CLOCKTIME:
			return "clocktime";
		case COORDINATE:
			return "coordinate";
		case DATE:
			return "date";
		case DOUBLE:
			return "double";
		case FLOAT:
			return "float";
		case INTEGER:
			return "integer";
		case LIST:
			return "list";
		case LONG:
			return "long";
		case MEMO:
			return "memo";
		case MONEY:
			return "money";
		case PERCENTAGE:
			return "percentage";
		case STOPWATCHTIME:
			return "stopwatchtype";
		case IMMUTABLE:
			return "immutable";
		case ENUM:
			return "enum";
		default:
			break;
		}
		throw new UnsupportedOperationException("Unknown type: "+type);

	}
	public static ValueType parseType(String type) {
		switch(type) {
		case  "memo":
			return ValueType.MEMO;
		case "money":
			return ValueType.MONEY;
		case "percentage":
			return ValueType.PERCENTAGE;
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
		case "float":
			return ValueType.FLOAT;
		case "boolean":
			return ValueType.BOOLEAN;
		case "binary_digest":
			return ValueType.BINARY_DIGEST;
		case "date":
		case "timestamp":
			return ValueType.DATE;
		case "clocktime":
			return ValueType.CLOCKTIME;
		case "list":
			return ValueType.LIST;
		case "binary":
			return ValueType.BINARY;
		case "coordinate":
			return ValueType.COORDINATE;
		case "object":
			return ValueType.UNKNOWN;
		case "enum":
			return ValueType.ENUM;
		}
		throw new UnsupportedOperationException("Unknown type: "+type);
	}

}
