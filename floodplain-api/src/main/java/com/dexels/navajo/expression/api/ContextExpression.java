package com.dexels.navajo.expression.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.script.api.MappableTreeNode;

import java.util.Optional;

public interface ContextExpression {

	public default Operand apply() {
		return apply(Optional.empty(),Optional.empty());
	}
	public default Operand apply(Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		return apply(null,immutableMessage,paramMessage);
	}


	public Operand apply(MappableTreeNode mapNode, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage);
	public boolean isLiteral();
	public Optional<String> returnType();
	public String expression();

}
