package com.dexels.oauth.impl;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Set;

import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.Token;

public class SimpleToken implements Token {
	
	private String clientId;
	private String code;
	private long expireTimestamp;
	private Set<Scope>scopes;
    private OauthUser user;
    protected Map<String, Object> attributes;
	private boolean isOpenID;

	

	public SimpleToken(String clientId, Set<Scope> scopes, OauthUser user, long expireTimestamp) {
		this.clientId = clientId;
		this.scopes = scopes;
		this.expireTimestamp = System.currentTimeMillis() + expireTimestamp;
		this.code = generateRandom();
		this.user = user;
	}

	private String generateRandom() {
		SecureRandom random = new SecureRandom();
		return new BigInteger(130, random).toString(32);
	}
	
	@Override
	public Map<String, Object> getAttributes() {
		return user.getAttributes();
	}

	@Override
	public long getExpireTimestamp() {
		return expireTimestamp;
	}

	@Override
	public boolean isExpired() {
		return System.currentTimeMillis() > getExpireTimestamp();
	}

	@Override
	public String getCode() {
		return code;
	}

	@Override
	public String getClientId() {
		return clientId;
	}

	@Override
	public Set<Scope> getScopes() {
		return scopes;
	}
	
	@Override
	public void setUsername(String username) {
	    return;
	}

    @Override
    public String getUsername() {
        return null;
    }

    @Override
    public OauthUser getUser() {
        return user;
    }
    
    @Override
    public void setIsOpenID(boolean isOpenID) {
    	this.isOpenID = isOpenID;
    }

	@Override
	public boolean isOpenID() {
		return isOpenID;
	}
}
