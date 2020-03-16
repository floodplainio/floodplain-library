package com.dexels.navajo.document.stream.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;

public interface RunningReactiveScripts {

	public void submit(StreamScriptContext context);

	public List<String> services();

	public void completed(StreamScriptContext context);

	public void cancel(String uuid);

	public Collection<StreamScriptContext> contexts();

	public JsonNode asJson();
	
	public void complete(String uuid);

}