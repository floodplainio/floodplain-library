package com.dexels.navajo.document;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.operand.Operand;

import java.util.Optional;

public interface ExpressionEvaluator {
  public Operand evaluate(String clause);
  public Operand evaluate(String clause, Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage);
}
