package com.dexels.oauth.api;

import java.util.List;

import com.dexels.oauth.api.exception.TokenStoreException;

public interface SSOTokenStore {
    public SSOToken createSSOToken(String clientId, OauthUser user, long expireAt, String code);

    public void insertSSOToken(SSOToken token) throws TokenStoreException;

    public void deleteSSOToken(final String code) throws TokenStoreException;;

    public SSOToken getSSOToken(final String code) throws TokenStoreException;

    public List<SSOToken> getSSOTokens(OauthUser user) throws TokenStoreException;


}
