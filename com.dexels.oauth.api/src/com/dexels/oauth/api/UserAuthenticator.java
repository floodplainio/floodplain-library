package com.dexels.oauth.api;

public interface UserAuthenticator {    
    /** Internal use only! */
    public OauthUser getUser(String username, Client client);
    
	public OauthUser getUser(String username, String password, Client client);
	public String getEmail(String username, Client client);
	
	public void linkUser(OauthUser user, String personid, String domain, Client client) throws Exception;
	public boolean userExists(String username, Client client);
	public void registerUser(String username, String password, Client client) throws Exception;
}
