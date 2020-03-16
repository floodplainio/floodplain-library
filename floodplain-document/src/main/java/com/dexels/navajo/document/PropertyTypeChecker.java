package com.dexels.navajo.document;

import com.dexels.navajo.document.typecheck.TypeChecker;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class PropertyTypeChecker {

  private static PropertyTypeChecker instance = null;

  private final Map<String,TypeChecker> propertyTypeCheckMap = new HashMap<>();

  private PropertyTypeChecker() {
  }

  public static PropertyTypeChecker getInstance() {
    if (instance==null) {
      instance = new PropertyTypeChecker();
    }
    return instance;
  }

  private void loadTypeChecker(String type) {

    if (type == null || type.equals("")) {
      propertyTypeCheckMap.put("",null);
      return;
    }

    String s = type.substring(1);
    String classname = "com.dexels.navajo.document.typecheck.TypeCheck"+type.substring(0,1).toUpperCase()+s;
    Class<?> c;
    try {
      c = Class.forName(classname);
    }
    catch (ClassNotFoundException ex) {
      propertyTypeCheckMap.put(type,null);
      return;
    }
    try {
      TypeChecker tc = (TypeChecker) c.getDeclaredConstructor().newInstance();
      propertyTypeCheckMap.put(type,tc);
    }
    catch (IllegalAccessException|InstantiationException|IllegalArgumentException|InvocationTargetException|NoSuchMethodException|SecurityException ex1) {
      propertyTypeCheckMap.put(type,null);
    }

  }

  private TypeChecker getTypeChecker(String type) {
    if (!propertyTypeCheckMap.containsKey(type)) {
      loadTypeChecker(type);
    }
    return propertyTypeCheckMap.get(type);
  }


  public String verify(Property p, String value)  {
    if (p==null) {
     return value;
    }
    if (p.getType()==null) {
      return value;
    }
    TypeChecker tc = getTypeChecker(p.getType());
    if (tc==null) {
      return value;
    }
    return tc.verify(p,value);
 }
}
