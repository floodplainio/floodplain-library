package com.dexels.oauth.web.exceptions;

public class OAuthInvalidScopeException extends OAuthException {
	private static final long serialVersionUID = 1729604570470666414L;

	public OAuthInvalidScopeException() {
		super("invalid_scope", "The requested scope is invalid, unknown, or malformed.");
	}
}
