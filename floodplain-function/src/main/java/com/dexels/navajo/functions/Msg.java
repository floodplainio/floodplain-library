package com.dexels.navajo.functions;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.util.Map.Entry;
import java.util.Set;

/**
 * <p>Title: Navajo Product Project</p>
 * <p>Description: This is the official source for the Navajo server</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels BV</p>
 * @author not attributable
 * @version 1.0
 */

public final class Msg  extends FunctionInterface {
  public Msg() {
  }
  @Override
	public boolean isPure() {
  		return false;
  }

      @Override
	public String remarks() {
          return "Will return a immutable message";
      }

      @Override
	public String usage() {
          return "Will return a immutable message";
      }

      @Override
	public final Object evaluate() throws TMLExpressionException {
    	  ImmutableMessage msg = ImmutableFactory.empty();
    	  Set<Entry<String, Operand>> entrySet = super.getNamedParameters().entrySet();
    	  for (Entry<String, Operand> e : entrySet) {
        	  Operand value = e.getValue();
    		  msg = msg.with(e.getKey(), value.value, value.type);
    	  }
        return msg;
      }
  }

