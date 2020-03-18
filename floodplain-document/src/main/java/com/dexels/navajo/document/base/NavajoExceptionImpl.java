package com.dexels.navajo.document.base;

import com.dexels.navajo.document.NavajoException;


public final class NavajoExceptionImpl extends NavajoException {
  
	private static final long serialVersionUID = 4942222966862889552L;
public NavajoExceptionImpl() {
  }
  public NavajoExceptionImpl(String message) {
    super(message);
  }
  public NavajoExceptionImpl(Throwable root) {
	super(root);
  }
  public NavajoExceptionImpl(String message, Throwable root) {
		super(message,root);
	 }
  
}