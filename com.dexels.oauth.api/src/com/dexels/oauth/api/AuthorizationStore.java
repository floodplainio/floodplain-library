package com.dexels.oauth.api;

import java.util.Set;

import com.dexels.oauth.api.exception.AuthorizationStoreException;

public interface AuthorizationStore {
	public void delete (final AuthorizationCode authorizationCode) throws AuthorizationStoreException;
	public void insert (final AuthorizationCode authorizationCode) throws AuthorizationStoreException;
	public AuthorizationCode getCode (final String code, final String clientId, final String secret) throws AuthorizationStoreException;

	public AuthorizationCode createAuthorizationCode(Client client, OauthUser user,
	        Set<Scope> scopes, String redirectURI, long expireTimestamp);
}
