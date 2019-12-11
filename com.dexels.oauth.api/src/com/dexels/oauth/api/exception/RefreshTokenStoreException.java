package com.dexels.oauth.api.exception;

public class RefreshTokenStoreException extends StoreException {
	private static final long serialVersionUID = -3184997709876869071L;

	public RefreshTokenStoreException () {
		super();
	}
	
	public RefreshTokenStoreException (String message) {
		super(message);
	}
}
