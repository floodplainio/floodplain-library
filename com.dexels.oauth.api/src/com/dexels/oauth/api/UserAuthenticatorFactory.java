package com.dexels.oauth.api;


public class UserAuthenticatorFactory {
	private static UserAuthenticator authenticator;
	
	public static void setInstance (UserAuthenticator authenticator) {
	    UserAuthenticatorFactory.authenticator = authenticator;
	}
	
	public static void clearInstance (UserAuthenticator authenticator) {
	    UserAuthenticatorFactory.authenticator = null;
    }
	
	/* 
	 * If the requested key is not found, but we do have a wildcard UserAuthenticator, return the wildcard one
	 */
	public static UserAuthenticator getInstance () {
	    return authenticator;
	}
}
