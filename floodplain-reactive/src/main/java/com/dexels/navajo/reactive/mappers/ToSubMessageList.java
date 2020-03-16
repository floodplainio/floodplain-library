package com.dexels.navajo.reactive.mappers;

import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveMerger;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;

import java.util.*;
import java.util.function.Function;

public class ToSubMessageList implements ReactiveMerger {

	public ToSubMessageList() {
	}

	@Override
	public Function<StreamScriptContext,Function<DataItem,DataItem>> execute(ReactiveParameters params) {
		return context->(item)->{
			ReactiveResolvedParameters resolved = params.resolve(context, Optional.empty(),item.stateMessage(), this);
			boolean condition = resolved.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}
			;
			return DataItem.of(item.stateMessage().withSubMessages(resolved.paramString("name"), item.messageList()), item.stateMessage());
		};
	
	}
	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"name","condition"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{"name"}));
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		Map<String,String> r = new HashMap<>();
		r.put("name", Property.STRING_PROPERTY);
		r.put("condition", Property.BOOLEAN_PROPERTY);
		return Optional.of(Collections.unmodifiableMap(r));
	}

	@Override
	public String name() {
		return "toSubMessageList";
	}

}
