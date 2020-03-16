package com.dexels.navajo.reactive.source.input;

import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;
import com.dexels.navajo.reactive.api.SourceMetadata;

import java.util.*;

public class TmlInputSourceFactory implements ReactiveSourceFactory, SourceMetadata {

	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		return new  TmlInputSource(this, parameters);
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"path"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{}));
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		Map<String,String> res = new HashMap<>();
		res.put("name", "string");
		return Optional.of(Collections.unmodifiableMap(res));
	}


	@Override
	public Type sourceType() {
		return Type.EVENTSTREAM;
	}

	@Override
	public String name() {
		return "tmlinput";
	}
}

