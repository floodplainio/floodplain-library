package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.FunctionInterface;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class Now extends FunctionInterface {

  @Override
public String remarks() {
    return "Now() returns the current timestamp as a string in the following format: yyyy/MM/dd HH:uu:mm";
  }
  @Override
public final Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {
    Date today = new Date();
    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    return format.format(today);
  }
  @Override
public String usage() {
    return "Now()";
  }

  public static void main(String [] args) throws Exception {
    Now n = new Now();
    n.reset();
    String result = (String) n.evaluate();
    System.err.println("result = " + result);
  }
}