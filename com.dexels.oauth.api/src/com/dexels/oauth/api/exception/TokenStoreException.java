package com.dexels.oauth.api.exception;

public class TokenStoreException extends StoreException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6064179740025144388L;

	public TokenStoreException() {
	}

	public TokenStoreException(String message) {
		super(message);
	}

	public TokenStoreException(Throwable cause) {
		super(cause);
	}

	public TokenStoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public TokenStoreException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
