package com.dexels.oauth.web;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.AuthorizationStore;
import com.dexels.oauth.api.AuthorizationStoreFactory;
import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.ClientStore;
import com.dexels.oauth.api.ClientStoreFactory;
import com.dexels.oauth.api.PasswordResetter;
import com.dexels.oauth.api.PasswordResetterFactory;
import com.dexels.oauth.api.RefreshTokenStore;
import com.dexels.oauth.api.RefreshTokenStoreFactory;
import com.dexels.oauth.api.SSOTokenStore;
import com.dexels.oauth.api.SSOTokenStoreFactory;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.ScopeStore;
import com.dexels.oauth.api.ScopeStoreFactory;
import com.dexels.oauth.api.TokenStore;
import com.dexels.oauth.api.TokenStoreFactory;
import com.dexels.oauth.api.exception.AuthorizationStoreException;
import com.dexels.oauth.api.exception.ClientStoreException;
import com.dexels.oauth.api.exception.ScopeStoreException;
import com.dexels.oauth.web.exceptions.OAuthServerErrorException;

public class OAuthCommandBase {

	private HttpServletRequest request;
	private HttpServletResponse response;
	
	private static final  String OAUTH_TEMPLATE = "oauth_template";
    public static final String NAVAJOSESSION_COOKIE = "NAVAJOSESSION";

    
	public static final long TOKEN_EXPIRE_TIMESTAMP = 3600000;
	public static final long REFRESH_TOKEN_EXPIRE_TIMESTAMP = 1036800000L; //12 days
	public static final String TOKEN_TYPE = "bearer";
	
	public OAuthCommandBase(HttpServletRequest request, HttpServletResponse response) {
		setRequest(request);
		setResponse(response);
	}
	
	public OAuthSession getTemplate () {
		return (OAuthSession) getRequest().getSession().getAttribute(OAUTH_TEMPLATE);
	}
	
	public void setTemplate (OAuthSession template) {
		getRequest().getSession().setAttribute(OAUTH_TEMPLATE, template);
	}
	
	public String getRequestParameter (String key) {
		String parameter = getRequest().getParameter(key);
		
		if (parameter != null && parameter.length() != 0)
			return parameter;
		return null;
	}
	
	public void sendToSessionExpired() throws IOException {
        sendAbsoluteRedirect("/auth/session_expired.html");
	}
	
    public void sendToLogin(OAuthSession template) throws IOException {
    	if (template.getClient().getApplication().isSingleAccount()) {
    		sendAbsoluteRedirect("/auth/login.html#" + template.getClient().getInstance());
    	}  else {
    		sendAbsoluteRedirect("/auth/club/login.html");
    	}
    }
    
    public void sendToAuthorize(OAuthSession template) throws IOException {
    	if (template.getClient().getApplication().isSingleAccount()) {
    		sendAbsoluteRedirect("/auth/authorize.html#" + template.getClient().getInstance());
    	} else {
    		sendAbsoluteRedirect("/auth/club/authorize.html");
    	}
    }
    
    public void sendToLinkIdentity(OAuthSession template) throws IOException {
        sendAbsoluteRedirect("/auth/link.html#" + template.getClient().getInstance());
    }
    
    public void setInfo (String title, String message) {
		getRequest().getSession().setAttribute("info", message);
		getRequest().getSession().setAttribute("infoheader", title);
	}
	
	public void setError (String title, String message) {
		getRequest().getSession().setAttribute("error", message);
		getRequest().getSession().setAttribute("errorheader", title);
	}
	
	public void clearError () {
		getRequest().getSession().setAttribute("error", null);
		getRequest().getSession().setAttribute("errorheader", null);
	}
	
	public void clearInfo() {
		getRequest().getSession().setAttribute("info", null);
		getRequest().getSession().setAttribute("infoheader", null);
	}
	
	private void sendAbsoluteRedirect (String relative) throws IOException {
		String forwardedProto = getRequest().getHeader("X-Forwarded-Proto");
		String host = getRequest().getHeader("Host");
		
		if (forwardedProto == null)
			forwardedProto = "http";
		
		getResponse().sendRedirect(String.format("%s://%s%s", forwardedProto, host, relative));
	}
	
	public AuthorizationCode getCode (String code, String clientId, String secret) throws OAuthServerErrorException {
		try {
			return getAuthorizationStore().getCode(code, clientId, secret);
		} catch (AuthorizationStoreException e) {
			throw new OAuthServerErrorException("Authorization store unreachable");
		}
	}
	
	public Client getClient (String clientId) throws OAuthServerErrorException {
		try {
			return getClientStore().getClient(clientId);
		} catch (ClientStoreException e) {
			e.printStackTrace();
			throw new OAuthServerErrorException("Client store unreachable");
		}
	}
	
	public Scope getScope (String scopeId) throws OAuthServerErrorException {
		try {
			return getScopeStore().getScope(scopeId);
		} catch (ScopeStoreException e) {
			throw new OAuthServerErrorException("Scope store unreachable");
		}
	}
	
	public PasswordResetter getPasswordResetter () {
		return PasswordResetterFactory.getInstance();
	}
	
	public TokenStore getTokenStore () {
		return TokenStoreFactory.getInstance();
	}
	
    public SSOTokenStore getSSOTokenStore() {
        return SSOTokenStoreFactory.getInstance();
    }

	public RefreshTokenStore getRefreshTokenStore() {
		return RefreshTokenStoreFactory.getInstance();
	}
	
	public AuthorizationStore getAuthorizationStore () {
		return AuthorizationStoreFactory.getInstance();
	}
	

	public ScopeStore getScopeStore() {
		return ScopeStoreFactory.getInstance();
	}
	
	public ClientStore getClientStore() {
		return ClientStoreFactory.getInstance();
	}

	public HttpServletRequest getRequest() {
		return request;
	}

	public void setRequest(HttpServletRequest request) {
		this.request = request;
	}

	public HttpServletResponse getResponse() {
		return response;
	}

	public void setResponse(HttpServletResponse response) {
		this.response = response;
	}
}
