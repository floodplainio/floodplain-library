package com.dexels.oauth.web.exceptions;

public class OAuthInvalidGrantException extends OAuthException {
	private static final long serialVersionUID = -3098784782972170552L;

	public OAuthInvalidGrantException() {
		super("invalid_grant", "The provided authorization grant (e.g., authorization code, resource owner credentials) or refresh token is invalid, expired, revoked, does not match the redirection URI used in the authorization request, or was issued to another client");
	}	
}
