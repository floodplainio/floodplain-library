package com.dexels.oauth.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dexels.oauth.api.exception.TokenStoreException;

public interface TokenStore {	
    @Deprecated
	public OAuthToken createOAuthToken(String clientId, Set<Scope> scopes, Map<String, Object>attributes, long expireTimestamp, String type);
	public OAuthToken createOAuthToken(String clientId, OauthUser user, Set<Scope> scopes, String type, long expireTimestamp);
	public OAuthToken createOAuthToken(AuthorizationCode code, String type, long expireTimestamp);
	public OAuthToken createOAuthToken(RefreshToken token, String type, long expireTimestamp);
	
	public OAuthToken getToken(final String code) throws TokenStoreException;
	
	/**
	 * @return Returns a list of tokens that belong to the same user and clientid
	 * 	 */
	public List<OAuthToken> findTokens(String clientId, Integer userid) throws TokenStoreException;
	public List<OAuthToken> findTokens(Integer userid) throws TokenStoreException;
	
	public void delete(final String code) throws TokenStoreException;
	public void insert(final OAuthToken token) throws TokenStoreException;
	public void update(final OAuthToken token) throws TokenStoreException;
	
	/** Invalidate the cache for the token */
	public void refresh(final String token);
}
