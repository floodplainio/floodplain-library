package com.dexels.navajo.expression.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.navajo.document.operand.Operand;

import java.util.Optional;

public interface ContextExpression {

	public default Operand apply() {
		return apply(Optional.empty(),Optional.empty());
	}
//	public default Operand apply(Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
//		return apply(null,immutableMessage,paramMessage);
//	}


	public Operand apply(Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage);
	public boolean isLiteral();
	public Optional<ValueType> returnType();
	public String expression();

}
