package com.dexels.oauth.api;

public interface AuthorizationCode extends Token {
	public String getRedirectURI();
}
