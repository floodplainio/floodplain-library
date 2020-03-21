package com.dexels.navajo.parser.compiled.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.reactive.api.ReactivePipe;
import com.dexels.navajo.reactive.api.ReactiveSource;

import java.util.List;
import java.util.Optional;

public class ReactivePipeNode implements ContextExpression {
	private final Operand actual;

	public ReactivePipeNode(Optional<ReactiveSource> source, List<Object> transformers) {
		this.actual = source.isPresent() ? 
			new Operand(new ReactivePipe(source.get(), transformers),ImmutableMessage.ValueType.REACTIVEPIPE)
				:
				new Operand(transformers, ImmutableMessage.ValueType.REACTIVEPARTIALPIPE);
		
	}

	@Override
	public Operand apply(Optional<ImmutableMessage> immutableMessage,
			Optional<ImmutableMessage> paramMessage) {
		return actual;
	}

	public boolean isStreamInput() {
		return ((ReactivePipe)this.actual.value).source.streamInput();
	}
	@Override
	public boolean isLiteral() {
		return true;
	}

	@Override
	public Optional<ImmutableMessage.ValueType> returnType() {
		return Optional.of(ImmutableMessage.ValueType.REACTIVEPIPE);
	}

	@Override
	public String expression() {
		return "some_reactive_expression";
	}
}
