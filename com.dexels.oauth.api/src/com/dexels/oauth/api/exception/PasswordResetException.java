package com.dexels.oauth.api.exception;

public class PasswordResetException extends StoreException {
	private static final long serialVersionUID = 895976760251134138L;


	public PasswordResetException() {
	
	}

	public PasswordResetException(String message) {
		super(message);
	}

	public PasswordResetException(Throwable cause) {
		super(cause);
	}

	public PasswordResetException(String message, Throwable cause) {
		super(message, cause);
	}

	public PasswordResetException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
