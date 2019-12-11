package com.dexels.oauth.api;

public class ScopeAcceptanceStoreFactory {
    private static ScopeAcceptanceStore scopeStore;
        
    public static void setInstance (ScopeAcceptanceStore store) {
        ScopeAcceptanceStoreFactory.scopeStore = store;
    }
    
    public static void clearInstance (ScopeAcceptanceStore store) {
        ScopeAcceptanceStoreFactory.scopeStore = null;
    }

    public static ScopeAcceptanceStore getInstance () {
        return scopeStore;
    }
}
