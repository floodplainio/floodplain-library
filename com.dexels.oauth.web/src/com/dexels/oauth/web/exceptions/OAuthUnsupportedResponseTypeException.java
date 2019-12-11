package com.dexels.oauth.web.exceptions;

public class OAuthUnsupportedResponseTypeException extends OAuthException {
	private static final long serialVersionUID = 2892976830389640880L;

	public OAuthUnsupportedResponseTypeException() {
		super("unsupported_response_type", "The authorization server does not support obtaining an authorization code using this method.");
	}
}
