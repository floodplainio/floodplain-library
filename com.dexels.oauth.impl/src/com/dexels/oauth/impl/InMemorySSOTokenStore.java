package com.dexels.oauth.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.SSOToken;
import com.dexels.oauth.api.SSOTokenStore;
import com.dexels.oauth.api.SSOTokenStoreFactory;
import com.dexels.oauth.api.exception.TokenStoreException;

@Component(name="dexels.oauth.ssotokenstore.inmemory",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class InMemorySSOTokenStore implements SSOTokenStore {
    private Map<String, SSOToken> tokens = new HashMap<>();
    
    @Activate
    public void activate() {
        SSOTokenStoreFactory.setInstance(this);
    }

    @Override
    public SSOToken createSSOToken(final String clientId, final OauthUser user, final long expireAt, final String code) {
        return new SimpleSSOToken(clientId, user, expireAt, code);
    }

    @Override
    public void insertSSOToken(SSOToken token) throws TokenStoreException {
        tokens.put(token.getCode(), token);
    }

    @Override
    public void deleteSSOToken(String code) throws TokenStoreException {
        tokens.remove(code);
        
    }

    @Override
    public SSOToken getSSOToken(String code) throws TokenStoreException {
        return tokens.get(code);
    }

    @Override
    public List<SSOToken> getSSOTokens(OauthUser user) throws TokenStoreException {
        List<SSOToken> result = new ArrayList<>();
        for (SSOToken token : tokens.values()) {
            if (token.getUsername().equals(user.getUsername()) && token.getUserId() == user.getUserId()) {
                result.add(token);
            }
        }
        return result;
    }

}
