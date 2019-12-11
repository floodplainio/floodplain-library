package com.dexels.oauth.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.ScopeStore;
import com.dexels.oauth.api.ScopeStoreFactory;
import com.dexels.oauth.api.exception.ScopeStoreException;
				
@Component(name="dexels.oauth.scopestore.inmemory",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class InMemoryScopeStore implements ScopeStore {

	private final Map<String, Scope> scopes = new HashMap<>();
	
	@Activate
	public void activate () {
		ScopeStoreFactory.setInstance(this);
	}
	
	@Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeScope", cardinality=ReferenceCardinality.MULTIPLE)
	public void addScope(Scope scope) {
	    scopes.put(scope.getId(), scope);
	}
	
	public void removeScope(Scope scope) {
	    scopes.remove(scope.getId());
	}

	@Override
	public Scope getScope(String id) throws ScopeStoreException {
		return scopes.get(id);
	}

    @Override
    public List<Scope> getScopes() {
        return new ArrayList<>(scopes.values());
    }
}
