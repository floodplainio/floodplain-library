package com.dexels.oauth.impl;

import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.RefreshToken;

public class SimpleRefreshToken extends SimpleToken implements RefreshToken {
	
	private String oauthTokenCode;

	public SimpleRefreshToken(OAuthToken token, long expireTimestamp) {
		super(token.getClientId(), token.getScopes(), token.getUser(), expireTimestamp);
		oauthTokenCode = token.getCode();
	}

	@Override
	public String getOAuthTokenCode() {
		return oauthTokenCode;
	}

	@Override
	public boolean updateUser() {
		return false;
	}

    @Override
    public void setUpdateUser(boolean update) {        
    }
}
