package com.dexels.oauth.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.RefreshToken;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.TokenStore;
import com.dexels.oauth.api.TokenStoreFactory;
import com.dexels.oauth.api.exception.TokenStoreException;
import com.dexels.oauth.api.OauthUser;

@Component(name = "dexels.oauth.tokenstore.inmemory", configurationPolicy = ConfigurationPolicy.REQUIRE)
public class InMemoryTokenStore implements TokenStore {

    private final Map<String, OAuthToken> tokens = new HashMap<>();

    @Activate
    public void activate() {
        TokenStoreFactory.setInstance(this);
    }

    @Override
    public OAuthToken createOAuthToken(AuthorizationCode code, String type, long expireTimestamp) {
        return new SimpleOAuthToken(code, type, expireTimestamp);
    }

    @Override
    public OAuthToken createOAuthToken(RefreshToken token, String type, long expireTimestamp) {
        return new SimpleOAuthToken(token, type, expireTimestamp);
    }

    @Override
    public OAuthToken getToken(String code) throws TokenStoreException {
        return tokens.get(code);
    }

    @Override
    public void delete(String code) throws TokenStoreException {
        tokens.remove(code);
    }

    @Override
    public void insert(OAuthToken token) throws TokenStoreException {
        tokens.put(token.getCode(), token);
    }

    @Override
    public OAuthToken createOAuthToken(String clientId, OauthUser user, Set<Scope> scopes, String type,
            long expireTimestamp) {
        return new SimpleOAuthToken(clientId, scopes, user, expireTimestamp, type);
    }

    @Override
    public OAuthToken createOAuthToken(String clientId, Set<Scope> scopes, Map<String, Object> attributes,
            long expireTimestamp, String type) {
        return new SimpleOAuthToken(clientId, scopes, attributes, expireTimestamp, type);
    }

    @Override
    public List<OAuthToken> findTokens(String clientId, Integer userid) throws TokenStoreException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public List<OAuthToken> findTokens(Integer userid) throws TokenStoreException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void update(OAuthToken token) throws TokenStoreException {
        tokens.put(token.getCode(), token);
    }

    @Override
    public void refresh(String token){
        // not applicable - no caching
        
    }


}
