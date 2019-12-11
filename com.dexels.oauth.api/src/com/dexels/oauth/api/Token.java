package com.dexels.oauth.api;

import java.util.Map;
import java.util.Set;

public interface Token {
	public long getExpireTimestamp();
	
	public boolean isExpired();
	public void setIsOpenID(boolean isOpenID);
	public boolean isOpenID();
	public void setUsername(String username);
	public String getUsername();
	public String getCode();
	public String getClientId();
	
	public Set<Scope> getScopes();
	
	/**
	 *
	 * @deprecated use {@link #getUser()} instead.  
	 */
	@Deprecated
	public Map<String, Object> getAttributes();
	
	public OauthUser getUser();
}
