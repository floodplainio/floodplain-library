package com.dexels.oauth.web;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.AuthorizationStore;
import com.dexels.oauth.api.ClientStore;
import com.dexels.oauth.api.PasswordResetter;
import com.dexels.oauth.api.RefreshTokenStore;
import com.dexels.oauth.api.SSOTokenStore;
import com.dexels.oauth.api.ScopeStore;
import com.dexels.oauth.api.TokenStore;
import com.dexels.oauth.api.UserAuthenticator;
import com.dexels.oauth.web.commands.OAuthCommandFactory;

@Component(name="dexels.oauth.servlet",service=Servlet.class, immediate=true, property={"alias=/oauth","servlet-name=oauth"},configurationPolicy=ConfigurationPolicy.OPTIONAL)
public class OauthServlet extends HttpServlet {
	
	private static final long serialVersionUID = -1948354354961917987L;
    private final static Logger logger = LoggerFactory.getLogger(OauthServlet.class);
	
	private ClientStore clientStore;
	private UserAuthenticator userAuthenticator;
	private AuthorizationStore authorizationStore;
	private ScopeStore scopeStore;
	private TokenStore tokenStore;
    private SSOTokenStore ssoTokenStore;
	private RefreshTokenStore refreshTokenStore;
	private PasswordResetter passwordResetter;	
	
	@Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        try {
            OAuthCommandFactory.create(request.getPathInfo(), request, response).execute();
        } catch (Throwable t) {
            logger.error("Exception in handling oauth request", t);
            throw new ServletException(t.getMessage());
        }
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearPasswordResetter")
	public void setPasswordResetter (PasswordResetter passwordResetter) {
		this.passwordResetter = passwordResetter;
	}
	
	public void clearPasswordResetter(PasswordResetter passwordResetter) {
		this.passwordResetter = null;
	}
	
	public PasswordResetter getPasswordResetter () {
		return passwordResetter;
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearRefreshTokenStore")
	public void setRefreshTokenStore (RefreshTokenStore refreshTokenStore) {
		this.refreshTokenStore = refreshTokenStore;
	}
	
	public void clearRefreshTokenStore(RefreshTokenStore refreshTokenStore) {
		this.refreshTokenStore = null;
	}
	
	public RefreshTokenStore getRefreshTokenStore () {
		return refreshTokenStore;
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearClientStore")
	public void setClientStore (ClientStore clientStore) {
		this.clientStore = clientStore;
	}
	
	public void clearClientStore(ClientStore clientStore) {
		this.clientStore = null;
	}
	
	public ClientStore getClientStore () {
		return clientStore;
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearUserAuthenticator", cardinality=ReferenceCardinality.AT_LEAST_ONE)
	public void setUserAuthenticator(UserAuthenticator userAuthenticator) {
		this.userAuthenticator = userAuthenticator;
	}
	
	public void clearUserAuthenticator(UserAuthenticator userAuthenticator) {
		this.userAuthenticator = null;
	}
	
	public UserAuthenticator getUserAuthenticator () {
		return userAuthenticator;
	}

	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearAuthorizationStore")
	public void setAuthorizationStore(AuthorizationStore authorizationStore) {
		this.authorizationStore = authorizationStore;
	}
	
	public void clearAuthorizationStore(AuthorizationStore authorizationStore) {
		this.authorizationStore = null;
	}
	
	public AuthorizationStore getAuthorizationStore () {
		return authorizationStore;
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearScopeStore")
	public void setScopeStore(ScopeStore scopeStore) {
		this.scopeStore = scopeStore;
	}
	
	public void clearScopeStore(ScopeStore scopeStore) {
		this.scopeStore = null;
	}
	
	public ScopeStore getScopeStore () {
		return scopeStore;
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearTokenStore")
	public void setTokenStore(TokenStore tokenStore) {
		this.tokenStore = tokenStore;
	}

	public void clearTokenStore(TokenStore tokenStore) {
		this.tokenStore = null;
	}
	
	public TokenStore getTokenStore () {
		return tokenStore;
	}

    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "clearSSOTokenStore")
    public void setSSOTokenStore(SSOTokenStore ssoTokenStore) {
        this.ssoTokenStore = ssoTokenStore;
    }

    public void clearSSOTokenStore(SSOTokenStore ssoTokenStore) {
        this.ssoTokenStore = null;
    }

    public SSOTokenStore getSSOTokenStore() {
        return ssoTokenStore;
    }
}
