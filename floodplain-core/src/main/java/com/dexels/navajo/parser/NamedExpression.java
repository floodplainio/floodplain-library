package com.dexels.navajo.parser;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.document.Selection;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.expression.api.TipiLink;
import com.dexels.navajo.script.api.Access;
import com.dexels.navajo.script.api.MappableTreeNode;

import java.util.Optional;

public class NamedExpression implements ContextExpression {
	public final String name;
	public final ContextExpression expression;
	public final boolean isParam;

	public NamedExpression(String name, boolean isParam, ContextExpression expression) {
		this.name = name;
		this.isParam = isParam;
		this.expression = expression;
	}

	@Override
	public Operand apply(MappableTreeNode mapNode, Optional<ImmutableMessage> immutableMessage,
			Optional<ImmutableMessage> paramMessage) throws TMLExpressionException {
		return expression.apply(mapNode,immutableMessage,paramMessage);
	}

	@Override
	public boolean isLiteral() {
		return expression.isLiteral();
	}

	@Override
	public Optional<String> returnType() {
		return expression.returnType();
	}

	@Override
	public String expression() {
		return expression.expression();
	}
}
