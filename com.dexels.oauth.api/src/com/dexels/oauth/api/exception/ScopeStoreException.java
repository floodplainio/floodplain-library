package com.dexels.oauth.api.exception;

public class ScopeStoreException extends StoreException {
	private static final long serialVersionUID = -6481279311715436719L;

	public ScopeStoreException () {
		super();
	}
	
	public ScopeStoreException (String message) {
		super(message);
	}
}
