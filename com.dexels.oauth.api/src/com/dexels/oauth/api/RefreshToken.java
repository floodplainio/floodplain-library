package com.dexels.oauth.api;

public interface RefreshToken extends Token {
	public String getOAuthTokenCode();
	
	/** Indicates whether 'core' attributes of the user to which the token 
	 * belongs to have changed
	 * 
	 */
	public boolean updateUser();
	public void setUpdateUser(boolean update);
}
