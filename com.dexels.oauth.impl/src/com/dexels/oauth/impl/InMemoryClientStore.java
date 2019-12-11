package com.dexels.oauth.impl;

import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.ClientStore;
import com.dexels.oauth.api.ClientStoreFactory;
import com.dexels.oauth.api.exception.ClientStoreException;

@Component(name="dexels.oauth.clientstore.inmemory",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class InMemoryClientStore implements ClientStore {
	
	private final Map<String, Client> clients = new HashMap<String, Client>();
	
	@Activate
	public void activate () {
		ClientStoreFactory.setInstance(this);
	}
	
	@Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeClient", cardinality=ReferenceCardinality.MULTIPLE)
	public void addClient(Client client) {
	    clients.put(client.getId(), client);
	}
	
	public void removeClient(Client client) {
	    clients.remove(client.getId());
	}
	
	@Override
	public Client getClient(String clientId) throws ClientStoreException {
		return clients.get(clientId);
	}
}