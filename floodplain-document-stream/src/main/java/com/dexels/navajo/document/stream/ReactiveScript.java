package com.dexels.navajo.document.stream;

import com.dexels.navajo.document.stream.api.StreamScriptContext;
import io.reactivex.Flowable;

import java.util.List;
import java.util.Optional;

public interface ReactiveScript {
	public Flowable<Flowable<DataItem>> execute(StreamScriptContext context);
	public DataItem.Type dataType();
	public Optional<String> binaryMimeType();
	public boolean streamInput();
//	public Optional<String> streamMessage();
	public List<ReactiveParseProblem> problems();
	public List<String> methods();
	
}
