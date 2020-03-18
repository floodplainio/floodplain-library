package com.dexels.navajo.document.typecheck;

import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.PropertyTypeException;

public abstract class TypeChecker {
  public abstract String verify(Property p, String value) throws PropertyTypeException;
  public abstract String getType();

}
