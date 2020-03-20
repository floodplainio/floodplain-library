package com.dexels.immutable.factory;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.impl.ImmutableMessageImpl;
import com.dexels.immutable.impl.JSONImmutableMessageParserImpl;
import com.dexels.immutable.json.ImmutableJSON;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
@ApplicationScoped
public class ImmutableFactory {

	private static ImmutableMessageParser instance;
	private static final ImmutableMessage empty = create(Collections.emptyMap(),Collections.emptyMap(),Collections.emptyMap(),Collections.emptyMap());

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
}
