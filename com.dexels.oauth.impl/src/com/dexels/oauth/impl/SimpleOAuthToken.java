package com.dexels.oauth.impl;

import java.util.Map;
import java.util.Set;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.RefreshToken;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.OauthUser;

public class SimpleOAuthToken extends SimpleToken implements OAuthToken  {

	private String type;
	
	public SimpleOAuthToken(String clientId, Set<Scope> scopes, OauthUser user, long expireTimestamp, String type) {
		super(clientId, scopes, user, expireTimestamp);
		this.type = type;
	}

	public SimpleOAuthToken(RefreshToken token, String type, long expireTimestamp) {
		super(token.getClientId(), token.getScopes(), token.getUser(), expireTimestamp);
		this.type = type;
	}

	public SimpleOAuthToken(AuthorizationCode code, String type, long expireTimestamp) {
		super(code.getClientId(), code.getScopes(), code.getUser(), expireTimestamp);
		this.type = type;
	}

	public SimpleOAuthToken(String clientId, Set<Scope> scopes, Map<String, Object> attributes, long expireTimestamp,
            String type) {
	    super(clientId, scopes, null, expireTimestamp);
	    this.attributes = attributes;
        this.type = type;
    }

    @Override
	public String getTokenType() {
		return type;
	}

	@Override
	public Map<String, Object> getAttributes() {
	    if (this.getUser() != null) {
	        return getUser().getAttributes();
	    }
	    return attributes;
	}
}
