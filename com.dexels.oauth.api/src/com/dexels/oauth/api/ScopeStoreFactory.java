package com.dexels.oauth.api;

public class ScopeStoreFactory {
	private static ScopeStore instance = null;
	
	public static void setInstance (ScopeStore store) {
		instance = store;
	}
	
	public static ScopeStore getInstance () {
		return instance;
	}
}
