package com.dexels.immutable.factory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.api.customtypes.CoordinateType;
import com.dexels.immutable.impl.ImmutableMessageImpl;
import com.dexels.immutable.impl.JSONImmutableMessageParserImpl;
import com.dexels.immutable.json.ImmutableJSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.util.*;

@ApplicationScoped
public class ImmutableFactory {

	private static ImmutableMessageParser instance;
	private static final ImmutableMessage empty = create(Collections.emptyMap(),Collections.emptyMap(),Collections.emptyMap(),Collections.emptyMap());
	private final static Logger logger = LoggerFactory.getLogger(ImmutableFactory.class);

	public static ImmutableMessage empty() {
		return empty;
	}

	public static ImmutableMessage create(Map<String,? extends Object> values, Map<String,ValueType> types) {
		return create(values,types,Collections.emptyMap(),Collections.emptyMap());
	}
	public static ImmutableMessage create(Map<String,? extends Object> values, Map<String, ValueType> types, Map<String,ImmutableMessage> submessage, Map<String,List<ImmutableMessage>> submessages) {
		return new ImmutableMessageImpl(values, types, submessage, submessages);
	}

	public static ImmutableMessage create(ImmutableMessage message1, ImmutableMessage message2, String key) {
		return new ImmutableMessageImpl(message1, message2, key);
	}
	
	public static ImmutableMessageParser createParser() {
		return new JSONImmutableMessageParserImpl();
	}
	
	public static String ndJson(ImmutableMessage msg) throws IOException {
		return ImmutableJSON.ndJson(msg);
	}
	
	public static void setInstance(ImmutableMessageParser parser) {
		instance = parser;
	}
	
	public static ImmutableMessageParser getInstance() {
		return instance;
	}

	public static ValueType resolveTypeFromValue(Object val) {
			if(val==null) {
//				t.put(e.getKey(), "unknown");
				throw new NullPointerException("Can't resolve type from null value");
			} else if(val instanceof Long) {
				return ValueType.LONG;
			} else if(val instanceof Double) {
				return ValueType.DOUBLE;
			} else if(val instanceof Integer) {
				return ValueType.INTEGER;
			} else if(val instanceof Float) {
				return ValueType.FLOAT;
			} else if(val instanceof Date) {
				return ValueType.DATE;
			} else if(val instanceof Boolean) {
				return ValueType.BOOLEAN;
			} else if(val instanceof String) {
				return ValueType.STRING;
			} else if (val instanceof CoordinateType) {
				return ValueType.COORDINATE;
			} else {
				logger.warn("Unknown type::: {}",val.getClass());
				return ValueType.UNKNOWN;
			}
	}
}
