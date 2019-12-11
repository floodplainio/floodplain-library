package com.dexels.oauth.api;

import com.dexels.oauth.api.exception.ClientStoreException;

public interface ClientStore {
	public Client getClient (final String clientId) throws ClientStoreException;
}
