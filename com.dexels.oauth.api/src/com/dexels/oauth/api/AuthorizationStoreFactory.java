package com.dexels.oauth.api;

public class AuthorizationStoreFactory {
	private static AuthorizationStore instance = null;
	
	public static void setInstance (AuthorizationStore store) {
		instance = store;
	}
	
	public static AuthorizationStore getInstance () {
		return instance;
	}
}
