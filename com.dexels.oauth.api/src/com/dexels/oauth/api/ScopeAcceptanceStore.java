package com.dexels.oauth.api;

import java.util.Set;

public interface ScopeAcceptanceStore {

    /** Returns whether this user has previously accepted the scopes for a specific client */
    public boolean hasAcceptedScopesFor(OauthUser user, Client c, Set<Scope> scopes);
    
    public void setAcceptedScopesFor(OauthUser user, Client c, Set<Scope> scopes);
}
