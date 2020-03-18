package com.dexels.navajo.document;

public class PropertyTypeException extends RuntimeException {
	private static final long serialVersionUID = -1565828252903897704L;
public PropertyTypeException(Property p, String message) {
    super("Property type exception: "+message+" property: "+p.getName()+" type: "+p.getType()+" subtype: "+p.getSubType()+" value: "+p.getValue());
  }
  public PropertyTypeException(Throwable cause, Property p, String message) {
    super("Property type exception: "+message+" property: "+p.getName()+" type: "+p.getType()+" subtype: "+p.getSubType()+" value: "+p.getValue(),cause);

  }
}
