package com.dexels.oauth.api;

import java.util.List;

import com.dexels.oauth.api.exception.ScopeStoreException;

public interface ScopeStore {
	public Scope getScope (String id) throws ScopeStoreException;

    public List<Scope> getScopes();
}
