package com.dexels.oauth.api;

public class RefreshTokenStoreFactory {
	private static RefreshTokenStore instance = null;
	
	public static void setInstance (RefreshTokenStore store) {
		instance = store;
	}
	
	public static RefreshTokenStore getInstance () {
		return instance;
	}
}
