package com.dexels.oauth.api.exception;

public class StoreException extends Exception {
	private static final long serialVersionUID = 6064179740025144388L;

	public StoreException() {
	
	}

	public StoreException(String message) {
		super(message);
	}

	public StoreException(Throwable cause) {
		super(cause);
	}

	public StoreException(String message, Throwable cause) {
		super(message, cause);
	}

	public StoreException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
