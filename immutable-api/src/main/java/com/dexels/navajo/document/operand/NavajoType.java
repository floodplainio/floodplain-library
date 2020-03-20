package com.dexels.navajo.document.operand;

import com.dexels.immutable.api.ImmutableMessage.ValueType;

import java.io.Serializable;

public abstract class NavajoType implements  Serializable {
	private static final long serialVersionUID = -112880355087638085L;

	public abstract boolean isEmpty();
    public final ValueType type;
	public NavajoType(ValueType type) {
        this.type = type;
	}

  public NavajoType() {
	  this.type = ValueType.UNKNOWN;
  }
  
  public String toTmlString() {
	  return toString();
  }
  



}
