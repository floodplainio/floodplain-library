package com.dexels.oauth.web;

import java.util.HashSet;
import java.util.Set;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.Scope;

public class OAuthSession {
    private Client client;
	private Set<Scope> scopes = new HashSet<>();
	private Set<Scope> defaultScopes = new HashSet<>();
	private String redirectUri;
	private String state;
	private String type;

	private boolean confirmScopes = false;
	private OauthUser user;
    private boolean keepSession;
	private boolean isOpenID;
    private boolean ssoLogin;
    private String nonce;
   
	
	public OAuthSession (Client client, Set<Scope> scopes, String redirectUri, String state, String type) {
		setClient(client);
		setScopes(scopes);
		setRedirectUri(redirectUri);
		setState(state);
		setType(type);
		
		if (client.getAttributes().containsKey(Client.CONFIRM_SCOPES_ATTRIBUTEKEY)) {
		    confirmScopes = (boolean) client.getAttributes().get(Client.CONFIRM_SCOPES_ATTRIBUTEKEY);
		}
	}
	
	public Client getClient() {
		return client;
	}

	public void setClient(Client client) {
		this.client = client;
	}

	public Set<Scope> getScopes() {
		return scopes;
	}
	public Set<Scope> getDefaultScopes() {
        return defaultScopes;
    }

	public void setScopes(Set<Scope> scopes) {
		this.scopes = scopes;
	}
	
	public void setDefaultScopes(Set<Scope> scopes) {
        this.defaultScopes = scopes;
    }

	public String getRedirectUri() {
		return redirectUri;
	}

	public void setRedirectUri(String redirectUri) {
		this.redirectUri = redirectUri;
	}
	
	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

    public boolean isConfirmScopes() {
        return confirmScopes;
    }

	public OauthUser getUser() {
		return user;
	}

	public void setUser(OauthUser user) {
		this.user = user;
	}
	
	public void setKeepSession(boolean keepSession) {
	    this.keepSession = keepSession;
	}
	public boolean keepSession() {
	    return keepSession;
	}

	public void setIsOpenID(boolean openid) {
		isOpenID = openid;
		
	}
	
	public boolean isOpenID() {
		return isOpenID;
	}

	public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }
    
    public void setIsSSOLogin(boolean ssoLogin) {
        this.ssoLogin = ssoLogin;
    }
    public boolean getIsSSOLogin() {
        return ssoLogin;
    }

}
