package com.dexels.navajo.tipi.dev.server.appmanager.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.TipiCallbackSession;

@Component(immediate=true,name="tipi.dev.appstore.manager",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class AppStoreManagerImpl implements AppStoreManager {
	//
	private String codebase;
	protected String clientid;
	protected String clientsecret;
	protected String organization;
	private String applicationName;
	private String manifestCodebase;
	private boolean authorize=true;
	
	private final Map<String, RepositoryInstance> repositories = new HashMap<String, RepositoryInstance>();
	private final Map<RepositoryInstance, Map<String, Object>> repositorySettings = new HashMap<RepositoryInstance, Map<String, Object>>();

	

	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeRepositoryInstance",policy=ReferencePolicy.DYNAMIC)
	public void addRepositoryInstance(RepositoryInstance a,
			Map<String, Object> settings) {
		repositories.put(a.getRepositoryName(), a);
		repositorySettings.put(a, settings);
	}

	public void removeRepositoryInstance(RepositoryInstance a) {
		repositories.remove(a.getRepositoryName());
		repositorySettings.remove(a);
	}
	
	@Activate
	public void activate(Map<String,Object> configuration) throws IOException {
		clientid = (String) configuration.get("tipi.store.clientid");
		clientsecret = (String) configuration.get("tipi.store.clientsecret");
		organization = (String) configuration.get("tipi.store.organization");
		applicationName = (String) configuration.get("tipi.store.applicationname");
		manifestCodebase = (String) configuration.get("tipi.store.manifestcodebase");
		codebase = (String)configuration.get("tipi.store.codebase");
		Object authorizeObject = configuration.get("authorize");
		if(authorizeObject instanceof Boolean) {
			Boolean auth = (Boolean)authorizeObject;
			if(auth!=null) {
				authorize = auth;
			}
		} else {
			// it's a string
			if (authorizeObject!=null && authorizeObject instanceof String) {
				String authString = ((String)authorizeObject).toLowerCase();
				if("true".equals(authString)) {
					authorize = true;
				}
				if("false".equals(authString)) {
					authorize = false;
				}
				
			} else {
				authorize = true;
			}

		}
	}

	@Override
	public String getCodeBase() {
		return codebase;
	}

	@Override
	public String getClientId() {
		return this.clientid;
	}


	@Override
	public String getClientSecret() {
		return this.clientsecret;
	}


	@Override
	public String getOrganization() {
		return this.organization;
	}
	
	@Override
	public String getApplicationName() {
		return this.applicationName;
	}
	
	@Override
	public String getManifestCodebase() {
		return this.manifestCodebase;
	}

	@Override
	public Set<String> listApplications() {
		
		return repositories.keySet();
	}


	@Override
	public boolean useAuthorization() {
		return authorize;
	}
	
	
	private final Map<String,TipiCallbackSession> callbackSessions = new HashMap<>();
	@Reference(unbind="removeTipiCallbackSession",cardinality=ReferenceCardinality.MULTIPLE,policy=ReferencePolicy.DYNAMIC)
	public void addTipiCallbackSession(TipiCallbackSession session) {
		callbackSessions.put(session.getSessionId(), session);
	}

	
	public void removeTipiCallbackSession(TipiCallbackSession session) {
		callbackSessions.remove(session.getSessionId());
	}

	@Override
	public int getSessionCount() {
		return callbackSessions.size();
	}

}
