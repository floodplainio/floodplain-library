package com.dexels.oauth.impl;

import java.util.Set;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.OauthUser;

public class SimpleAuthorizationCode extends SimpleToken implements AuthorizationCode {
	
	private String redirectURI;
	
	public SimpleAuthorizationCode(Client client, OauthUser user, Set<Scope> scopes, String redirectURI, long expireTimestamp) {
		super(client.getId(), scopes, user, expireTimestamp);
		this.redirectURI = redirectURI;
	}

	@Override
	public String getRedirectURI() {
		return redirectURI;
	}
}