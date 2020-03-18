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
	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Selection sel, TipiLink tl, Map<String, Object> params, Optional<ImmutableMessage> immutableMessage) {
		return evaluate(clause, inMessage, o, null, null, sel, tl, params, immutableMessage, Optional.empty());
	}

	public static final Operand evaluateImmutable(String clause, Navajo in, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		return evaluate(clause, in, null, null, null, null, null, null, immutableMessage, paramMessage);
	}
	
	
	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Message parent,
			Message paramParent, Selection sel, TipiLink tl, Map<String, Object> params, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		if (clause.trim().equals("")) {
			return new Operand(null, "", "");
		}
		return evaluator.evaluate(clause, inMessage, o,  parent, paramParent,sel,tl,params,immutableMessage,paramMessage);
	}

	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Message parent,
			Message paramParent, Selection sel, TipiLink tl, Map<String, Object> params) {
		return evaluate(clause, inMessage, o, parent, paramParent, sel, tl, params, Optional.empty(),Optional.empty());
	}
	@Deprecated
	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Message parent,
			Message paramParent, Selection sel, TipiLink tl) {
		return evaluate(clause, inMessage, o, parent, paramParent, sel, tl, null);
	}

	@Deprecated
	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Message parent,
			Selection sel, TipiLink tl) {
		return evaluate(clause, inMessage, o, parent, null, sel, tl, null);
	}

	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Message parent) {
		return evaluate(clause, inMessage, o, parent, null, null, null, null);
	}

	public static final Operand evaluate(String clause, Navajo inMessage, MappableTreeNode o, Message parent,
			Message parentParam) {
		return evaluate(clause, inMessage, o, parent, parentParam, null, null, null);
	}

	public static final Operand evaluate(String clause, Navajo inMessage) throws SystemException {
		return evaluate(clause, inMessage, null, null, null, null, null, null,Optional.empty(),Optional.empty());
	}

}
