package com.dexels.oauth.web.exceptions;

public class OAuthServerErrorException extends OAuthException {
	private static final long serialVersionUID = 1040042246060095258L;

	public OAuthServerErrorException (String message) {
		super("server_error", message);
	}
}
