package com.dexels.oauth.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.AuthorizationStore;
import com.dexels.oauth.api.AuthorizationStoreFactory;
import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.exception.AuthorizationStoreException;
import com.dexels.oauth.api.OauthUser;

@Component(name="dexels.oauth.authorizationstore.inmemory",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class InMemoryAuthorizationStore implements AuthorizationStore {
	
	private final Map<String, AuthorizationCode> codes = new HashMap<String, AuthorizationCode>();
	
	private final static Logger logger = LoggerFactory.getLogger(InMemoryAuthorizationStore.class);
	
	@Activate
	public void activate () {
		AuthorizationStoreFactory.setInstance(this);
	}

	@Override
	public void insert (AuthorizationCode authorizationCode) throws AuthorizationStoreException {
		codes.put(authorizationCode.getCode(), authorizationCode);
		logger.info("Added authorizationcode: {} codes: {}", authorizationCode.getCode(), codes.size());
	}

	@Override
	public AuthorizationCode getCode(String code, String clientId, String secret) {
		return codes.get(code);
	}

	@Override
	public void delete(AuthorizationCode authorizationCode)
			throws AuthorizationStoreException {
		codes.remove(authorizationCode.getCode());
	}

	@Override
	public AuthorizationCode createAuthorizationCode(Client client, OauthUser user,
			Set<Scope> scopes, String redirectURI, long expireTimestamp) {
		return new SimpleAuthorizationCode(client, user, scopes, redirectURI, expireTimestamp);
	}
}