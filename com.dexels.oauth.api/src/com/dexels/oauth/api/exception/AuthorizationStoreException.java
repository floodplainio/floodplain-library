package com.dexels.oauth.api.exception;

public class AuthorizationStoreException extends StoreException {
	private static final long serialVersionUID = 4820012327504375648L;

	public AuthorizationStoreException () {
		super();
	}
	
	public AuthorizationStoreException (String message) {
		super(message);
	}
}
