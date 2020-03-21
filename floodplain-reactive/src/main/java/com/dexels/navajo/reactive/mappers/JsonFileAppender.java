package com.dexels.navajo.reactive.mappers;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveMerger;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;


public class JsonFileAppender implements ReactiveMerger {

	
	private final static Logger logger = LoggerFactory.getLogger(JsonFileAppender.class);

	public JsonFileAppender() {
	}

	@Override
	public Function<StreamScriptContext, Function<DataItem, DataItem>> execute(ReactiveParameters params) {
		ImmutableMessageParser parser = ImmutableFactory.createParser();
		return context -> {
			
			return (item) -> {
				ReactiveResolvedParameters named = params.resolve(context,Optional.of(item.message()), item.stateMessage(), this);
				String  path = named.paramString("path");
				boolean condition = named.optionalBoolean("condition").orElse(true);
				if(!condition) {
					return item;
				}
				try(FileOutputStream fw = new FileOutputStream(path,true)) {
					byte[] data = parser.serialize(item.message());
					fw.write(data);
					fw.write("\n".getBytes(Charset.forName("UTF-8")));
					fw.close();
				} catch(IOException e) {
					logger.error("Error writing to file: "+path,e);
				}
				// TODO Fix
				return item;
			};
		};
	}
	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"path","condition"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{"path"}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("path",ImmutableMessage.ValueType.STRING,"condition", ImmutableMessage.ValueType.BOOLEAN));
	}

	@Override
	public String name() {
		return "jsonFileAppender";
	}
}
