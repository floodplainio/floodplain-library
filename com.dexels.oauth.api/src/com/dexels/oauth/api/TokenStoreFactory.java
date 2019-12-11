package com.dexels.oauth.api;

public class TokenStoreFactory {
	private static TokenStore instance = null;
	
	public static void setInstance (TokenStore store) {
		instance = store;
	}
	
	public static TokenStore getInstance () {
		return instance;
	}
}
