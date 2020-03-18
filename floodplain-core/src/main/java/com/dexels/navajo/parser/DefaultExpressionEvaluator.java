package com.dexels.navajo.parser;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.*;
import com.dexels.navajo.expression.api.TipiLink;
import com.dexels.navajo.script.api.MappableTreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
		return evaluate(clause,null,Optional.empty(),Optional.empty());
	}

	@Override
	public Operand evaluate(String clause,  Object mappableTreeNode, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) throws NavajoException {
		try {
			return Expression.evaluate(clause, (MappableTreeNode) mappableTreeNode, immutableMessage,paramMessage);
		} catch (Throwable ex) {

			throw NavajoFactory.getInstance()
					.createNavajoException("Parse error: " + ex.getMessage() + "\n while parsing: " + clause, ex);
		}
	}

}
