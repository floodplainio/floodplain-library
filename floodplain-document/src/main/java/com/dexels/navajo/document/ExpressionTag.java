package com.dexels.navajo.document;

public interface ExpressionTag extends java.io.Serializable, Comparable<ExpressionTag>, Cloneable {

  /**
   * Public constants for the property node.
   */
  public static final String EXPRESSION_DEFINITION = "expression";
  public static final String EXPRESSION_CONDITION = "condition";
  public static final String EXPRESSION_VALUE = "value";

  public String getValue();

  public void setValue(String s);

  public String getCondition();

  public void setCondition(String s);

   public Object getRef();

}