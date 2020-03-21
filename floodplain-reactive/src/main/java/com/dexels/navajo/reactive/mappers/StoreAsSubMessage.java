package com.dexels.navajo.reactive.mappers;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveMerger;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class StoreAsSubMessage implements ReactiveMerger {

	private static final Logger logger = LoggerFactory.getLogger(StoreAsSubMessage.class);

	public StoreAsSubMessage() {
	}

	@Override
	public Function<StreamScriptContext,Function<DataItem,DataItem>> execute(ReactiveParameters params) {
		return context->item->{
			ImmutableMessage message = item.message();
			ImmutableMessage stateMessage = item.stateMessage();
			ReactiveResolvedParameters parms = params.resolve(context, Optional.of(message),stateMessage, this);
			boolean debug = parms.optionalBoolean("debug").orElse(false);
			boolean condition = parms.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}
			if(debug) {
				logger.info("Store as Submessage.State:\n{}Input:\n{}",ImmutableFactory.getInstance().describe(stateMessage),ImmutableFactory.getInstance().describe(message));
			}
			Optional<String> nameOpt = parms.optionalString("name");
			if(nameOpt.isPresent()) {
				String name = nameOpt.get();
				ImmutableMessage assembled = message.withSubMessage(name, stateMessage);
				return DataItem.of(assembled, item.stateMessage());
			} else {
				return DataItem.of(message.merge(item.stateMessage(), Optional.empty()));
			}
		};
	}

	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList("name","condition","debug"));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("debug", ImmutableMessage.ValueType.BOOLEAN,"condition", ImmutableMessage.ValueType.BOOLEAN,"name", ImmutableMessage.ValueType.STRING));
	}

	@Override
	public String name() {
		return "storeAsSubMessage";
	}
}
