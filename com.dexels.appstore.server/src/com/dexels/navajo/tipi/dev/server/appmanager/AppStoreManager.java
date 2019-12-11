package com.dexels.navajo.tipi.dev.server.appmanager;

import java.util.Set;

public interface AppStoreManager {
	public String getCodeBase();

	public String getClientId();
	
	public String getClientSecret();
	
	// Members of this GitHub organization will be granted access
	public String getOrganization();

	public String getApplicationName();

	public String getManifestCodebase();

	public Set<String> listApplications();

	public boolean useAuthorization();
	
	public int getSessionCount();
}
