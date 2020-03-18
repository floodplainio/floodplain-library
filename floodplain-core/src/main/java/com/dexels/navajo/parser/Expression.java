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
import com.dexels.navajo.document.*;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.expression.api.TipiLink;
import com.dexels.navajo.parser.compiled.api.CachedExpressionEvaluator;
import com.dexels.navajo.script.api.MappableTreeNode;
import com.dexels.navajo.script.api.SystemException;

import java.util.*;

public final class Expression {
	public static final String ACCESS = "ACCESS";

	private static CachedExpressionEvaluator evaluator = new CachedExpressionEvaluator();
	public static boolean compileExpressions = true; // Enabled by default
	
	private Expression() {
		// no instances
	}

	public static final Operand evaluate(String clause) {
		return evaluate(clause,null,Optional.empty(),Optional.empty());
	}

	public static final Operand evaluate(String clause, MappableTreeNode o, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		if (clause.trim().equals("")) {
			return new Operand(null, "", "");
		}
		return evaluator.evaluate(clause,  o,immutableMessage,paramMessage);
	}


}
