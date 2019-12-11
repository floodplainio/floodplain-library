package com.dexels.oauth.api;

public class SSOTokenStoreFactory {
    private static SSOTokenStore instance = null;

    public static void setInstance (SSOTokenStore store) {
        instance = store;
    }
    
    public static void clearInstance (SSOTokenStore store) {
        instance = null;
    }

    public static SSOTokenStore getInstance () {
        return instance;
    }
}
