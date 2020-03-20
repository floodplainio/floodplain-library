/**
 * Title:        Navajo<p>
 * Description:  <p>
 * Copyright:    Copyright (c) Arjen Schoneveld<p>
 * Company:      Dexels<p>
 * @author Arjen Schoneveld
 * @version $Id$
 */
package com.dexels.navajo.parser;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.parser.compiled.api.CachedExpressionEvaluator;

import java.util.Optional;

public final class Expression {
	public static final String ACCESS = "ACCESS";

	private static CachedExpressionEvaluator evaluator = new CachedExpressionEvaluator();

	private Expression() {
		// no instances
	}

	public static final Operand evaluate(String clause) {
		return evaluate(clause,Optional.empty(),Optional.empty());
	}

	public static final Operand evaluate(String clause,Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		if (clause.trim().equals("")) {
			return new Operand(null, ImmutableMessage.ValueType.UNKNOWN);
		}
		return evaluator.evaluate(clause, immutableMessage,paramMessage);
	}


}
