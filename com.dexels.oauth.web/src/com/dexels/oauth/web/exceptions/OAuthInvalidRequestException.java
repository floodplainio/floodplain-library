package com.dexels.oauth.web.exceptions;

public class OAuthInvalidRequestException extends OAuthException {
	private static final long serialVersionUID = 6236038001020974258L;

	public OAuthInvalidRequestException(String message) {
		super("invalid_request", message);
	}
}
