package com.dexels.oauth.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.RefreshToken;
import com.dexels.oauth.api.RefreshTokenStore;
import com.dexels.oauth.api.RefreshTokenStoreFactory;
import com.dexels.oauth.api.exception.RefreshTokenStoreException;
import com.dexels.oauth.api.exception.TokenStoreException;

@Component(name="dexels.oauth.refreshtokenstore.inmemory",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class InMemoryRefreshTokenStore implements RefreshTokenStore {
	private final Map<String, RefreshToken> refreshes = new HashMap<String, RefreshToken>();

	@Activate
	public void activate () {
		RefreshTokenStoreFactory.setInstance(this);
	}
	
	@Override
	public RefreshToken createRefreshToken(OAuthToken token, long expireTimestamp) {
	    RefreshToken refresh = new SimpleRefreshToken(token, expireTimestamp);
	    refreshes.put(refresh.getCode(), refresh);
	    return refresh;
	}
	
	@Override
	public void insert (RefreshToken token) throws RefreshTokenStoreException {
		refreshes.put(token.getCode(), token);
	}
	
	public void delete (RefreshToken token) throws RefreshTokenStoreException {
		refreshes.remove(token.getCode());
	}
	
	@Override
	public void delete(OAuthToken token) throws RefreshTokenStoreException {
		for (RefreshToken refresh : refreshes.values()) {
			if (refresh.getOAuthTokenCode().equals(token.getCode()))
				refreshes.remove(refresh);
		}
	}

	@Override
	public RefreshToken getRefreshToken(String refreshToken, String clientId, String secret) {
		return refreshes.get(refreshToken);
	}

    @Override
    public RefreshToken getRefreshToken(OAuthToken token) throws TokenStoreException {
        return null;
    }

    @Override
    public List<RefreshToken> findTokens(String clientId, Integer userid) throws TokenStoreException {
        return null;
    }
    @Override
    public List<RefreshToken> findTokens(Integer userid) throws TokenStoreException {
        return null;
    }

    @Override
    public void update(RefreshToken token) throws RefreshTokenStoreException {
        refreshes.put(token.getCode(), token);
    }
}