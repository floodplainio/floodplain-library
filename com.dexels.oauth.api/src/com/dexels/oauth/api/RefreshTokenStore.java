package com.dexels.oauth.api;

import java.util.List;

import com.dexels.oauth.api.exception.RefreshTokenStoreException;
import com.dexels.oauth.api.exception.TokenStoreException;

public interface RefreshTokenStore {
    public void insert(RefreshToken token) throws RefreshTokenStoreException;

    public void delete(RefreshToken token) throws RefreshTokenStoreException;

    public void update(RefreshToken token) throws RefreshTokenStoreException;

    public void delete(OAuthToken token) throws RefreshTokenStoreException;

    public RefreshToken getRefreshToken(String refreshToken, String clientId, String secret) throws RefreshTokenStoreException;

    public RefreshToken getRefreshToken(OAuthToken token) throws TokenStoreException;

    public List<RefreshToken> findTokens(String clientId, Integer userid) throws TokenStoreException;

    public List<RefreshToken> findTokens(Integer userid) throws TokenStoreException;

    public RefreshToken createRefreshToken(OAuthToken token, long expireTimestamp);
}
