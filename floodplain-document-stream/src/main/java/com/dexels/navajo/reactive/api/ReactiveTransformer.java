package com.dexels.navajo.reactive.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import io.reactivex.FlowableTransformer;

import java.util.Optional;

public interface ReactiveTransformer {

	public FlowableTransformer<DataItem,DataItem> execute(StreamScriptContext context,Optional<ImmutableMessage> current, ImmutableMessage param);
	public TransformerMetadata metadata();
	public ReactiveParameters parameters();
	default public Optional<String> mimeType() {
		return Optional.empty();
	}
}
