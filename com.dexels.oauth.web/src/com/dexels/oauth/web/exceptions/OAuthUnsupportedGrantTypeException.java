package com.dexels.oauth.web.exceptions;

public class OAuthUnsupportedGrantTypeException extends OAuthException {
	private static final long serialVersionUID = -7780971535057943031L;

	public OAuthUnsupportedGrantTypeException() {
		super("unsupported_grant_type", "The authorization grant type is not supported by the authorization server.");
	}
}
