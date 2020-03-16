package com.dexels.navajo.reactive.api;

import com.dexels.navajo.document.stream.ReactiveParseProblem;

import java.util.List;

public interface ReactiveTransformerFactory extends TransformerMetadata {
	public ReactiveTransformer build(
			List<ReactiveParseProblem> problems,
			ReactiveParameters parameters
			);

}
