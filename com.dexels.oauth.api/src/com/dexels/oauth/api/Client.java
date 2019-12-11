package com.dexels.oauth.api;

import java.util.Map;
import java.util.Set;

public interface Client {
	public final String CLUB_EXTERNALID_ATTRIBUTEKEY = "CLUB_EXTERNALID";
	public final String CONFIRM_SCOPES_ATTRIBUTEKEY = "CONFIRM_SCOPES";

	public String getId();
	public String getSecret();
	public String getInstance();
	public String getDescription();
	public String getRedirectURL();
	public String getUsername();
	public Set<String> getAllowedGrantTypes();
	public Map<String, Object> getAttributes();
	
	public Set<Scope> getDefaultScopes();
	public Set<Scope> getAllowedScopes();
	
	public Application getApplication();
}
