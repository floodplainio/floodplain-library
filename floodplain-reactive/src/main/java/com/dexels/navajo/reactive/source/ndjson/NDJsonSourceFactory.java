package com.dexels.navajo.reactive.source.ndjson;

import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO Implement? Delete?
public class NDJsonSourceFactory implements ReactiveSourceFactory {

	@Override
	public Type sourceType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String name() {
		return "ndjson";
	}

}
