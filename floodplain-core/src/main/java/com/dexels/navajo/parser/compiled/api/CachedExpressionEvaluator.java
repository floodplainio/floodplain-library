package com.dexels.navajo.parser.compiled.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.ExpressionEvaluator;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.parser.DefaultExpressionEvaluator;
import com.dexels.navajo.script.api.MappableTreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class CachedExpressionEvaluator extends DefaultExpressionEvaluator implements ExpressionEvaluator {
    private static final Logger logger = LoggerFactory.getLogger(CachedExpressionEvaluator.class);

	@Override
	public Operand evaluate(String clause) {
		return evaluate(clause,null,Optional.empty(),Optional.empty());
	}

	@Override
	public Operand evaluate(String clause,  Object mappableTreeNode, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		ExpressionCache ce = ExpressionCache.getInstance();
		try {
			return ce.evaluate(clause,  (MappableTreeNode)mappableTreeNode,immutableMessage,paramMessage);
		} catch (TMLExpressionException e) {
            throw new TMLExpressionException("TML parsing issue: "+clause);
		}
	}

}
