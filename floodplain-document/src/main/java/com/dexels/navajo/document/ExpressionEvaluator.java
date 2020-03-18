package com.dexels.navajo.document;

import com.dexels.immutable.api.ImmutableMessage;

import java.util.Optional;

/**
 * <p>Title: Navajo Product Project</p>
 * <p>Description: This is the official source for the Navajo server</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels BV</p>
 * @author Frank Lyaruu
 * @version $Id$
 */

public interface ExpressionEvaluator {
  public Operand evaluate(String clause);
  public Operand evaluate(String clause, Object mappableTreeNode, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage);
}
