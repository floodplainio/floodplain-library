package com.dexels.navajo.parser;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.ExpressionEvaluator;
import com.dexels.navajo.document.operand.Operand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * <p>
 * Title: Navajo Product Project
 * </p>
 * <p>
 * Description: This is the official source for the Navajo server
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Dexels BV
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */

public class DefaultExpressionEvaluator implements ExpressionEvaluator {

	private static final Logger logger = LoggerFactory.getLogger(DefaultExpressionEvaluator.class);

	public DefaultExpressionEvaluator() {
		logger.debug("Creating defaultExpressionEvaluator service");
	}

	@Override
	public Operand evaluate(String clause) {
		return evaluate(clause,Optional.empty(),Optional.empty());
	}

	@Override
	public Operand evaluate(String clause, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		try {
			return Expression.evaluate(clause, immutableMessage,paramMessage);
		} catch (Throwable ex) {
			throw new RuntimeException("Issue when evaluating: "+clause,ex);
		}
	}

}
