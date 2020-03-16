package com.dexels.navajo.reactive.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import io.reactivex.Flowable;

import java.util.Optional;
import java.util.function.Function;

public interface ReactiveSource {
	public SourceMetadata metadata();
	public Flowable<DataItem> execute(StreamScriptContext context,Optional<ImmutableMessage> current, ImmutableMessage paramMessage);
//	public DataItem.Type finalType();
	public boolean streamInput();
	public final Function<String, ReactiveMerger> emptyReducerSupplier = (e->null);
	public Type sourceType();
	public ReactiveParameters parameters();
}
