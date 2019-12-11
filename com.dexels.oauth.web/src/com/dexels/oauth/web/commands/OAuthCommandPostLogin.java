package com.dexels.oauth.web.commands;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.OauthUserIdentity;
import com.dexels.oauth.api.SSOToken;
import com.dexels.oauth.api.SSOTokenStoreFactory;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.exception.AuthorizationStoreException;
import com.dexels.oauth.api.exception.TokenStoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.dexels.oauth.web.util.CookieUtil;
import com.dexels.oauth.web.util.JwtUtil;

public class OAuthCommandPostLogin extends OAuthCommandBase implements OAuthCommand {
	private static final Logger logger = LoggerFactory.getLogger(OAuthCommandPostLogin.class);
	public static final long AUTHORIZATION_TOKEN_EXPIRE_TIMESTAMP = 600000;

	public OAuthCommandPostLogin(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		OAuthSession template = getTemplate();
		
		if (template == null) {
			sendToSessionExpired();
			return;
		}
		
		Client client = template.getClient();
		if (client == null) {
		    logger.warn("Client id is null");
		    sendToLogin(template);
		    return;
		}
	    
		OauthUser user =  template.getUser();
		if (user == null) {
		    logger.warn("User is null");
            sendToLogin(template);
            return;
		}
		
        if (SSOTokenStoreFactory.getInstance() != null && !getTemplate().getIsSSOLogin()) {
            generateSSOToken(template, client, user);
        }
        
		if ("code".equals(template.getType())) {
			logger.debug("Following the code path");
			try {
				authorize(client, user, template);
			} catch (AuthorizationStoreException e) {
			    logger.error("AuthorizationException in authorize", e);
				setError("", "Server tijdelijk niet beschikbaar, probeer het later nog eens.");
                sendToLogin(template);
			}
			return;
		} else if ("token".equals(template.getType())) {
	        logger.debug("Following the token path");
            try {
                String redirect = processTokenFlow(template, client, user);
                logger.info("Sending {} to redirect: {}", user.getUsername(), redirect);
                getRequest().getSession().invalidate();
                getResponse().sendRedirect(redirect);
            } catch (TokenStoreException e) {
                logger.error("Inserting token failed", e);
                setError("", "Server tijdelijk niet beschikbaar, probeer het later nog eens.");
                sendToLogin(template);
            }
            return;
		} else {
		    logger.warn("Unsupported flow type: {}", template.getType());
		}
		
    	setError("", "Gebruiker incorrect");
        sendToLogin(template);

	}

    private void generateSSOToken(OAuthSession template, Client client, OauthUser user) {
        String ssoTokenCode = JwtUtil.generateSSOToken(user.getUsername() + ":" + user.getUserId());
        if (ssoTokenCode == null) {
            // Some error during generating a token...
            return;
        }
        Long maxAge = template.keepSession() ? 14 * 24 * 60 * 60L : 30L; // 14 days vs 30 seconds
        long expire = new Date().getTime() + (maxAge * 1000);

        String ipAddress = getRequest().getHeader("X-Forwarded-For");
        if (ipAddress == null) {
            ipAddress = getRequest().getRemoteAddr();
        }

        SSOToken token = SSOTokenStoreFactory.getInstance().createSSOToken(client.getId(), user, expire, ssoTokenCode);

        token.setIpAddress(ipAddress);
        token.setUserAgent(getRequest().getHeader("User-Agent"));
        try {
            SSOTokenStoreFactory.getInstance().insertSSOToken(token);
            CookieUtil.create(getResponse(), NAVAJOSESSION_COOKIE, ssoTokenCode, maxAge.intValue(), getHostname());

        } catch (TokenStoreException e) {
           logger.error("Unable to create sso token: ", e);
        }
    }

    private String getHostname() {
        String issuer = JwtUtil.getIssuer();
        // Remote http part
        issuer = issuer.substring(issuer.lastIndexOf('/')+1);
        if (issuer.indexOf(':') != -1) { 
            // Remove port
            issuer = issuer.substring(0,  issuer.indexOf(':'));
        }
        return issuer;
    }

    private String processTokenFlow(OAuthSession template, Client client, OauthUser user) throws TokenStoreException {
        Set<Scope> tokenScopes = template.getScopes();
        tokenScopes.addAll(template.getDefaultScopes());
        OAuthToken token = getTokenStore().createOAuthToken(client.getId(), user, tokenScopes, TOKEN_TYPE, TOKEN_EXPIRE_TIMESTAMP);
        if (user.getUsername() != null) {
            token.setUsername(user.getUsername());
        }
        
        List<String>scopeIds = new ArrayList<>();
        for (Scope scope : tokenScopes) {
            scopeIds.add(scope.getId());
        }
        String scopes = String.join(",", scopeIds).trim();
        
        String redirect = String.format("%s#token=%s&token_type=%s&scope=%s&expires_in=%s", template.getRedirectUri(), token.getCode(),
                TOKEN_TYPE, scopes, TOKEN_EXPIRE_TIMESTAMP / 1000);
        if (template.getState() != null) redirect += "&state=" + template.getState();
        
//        if (template.isOpenID()) {
//            token.setIsOpenID(true);
//            redirect += "&id_token=" + JwtUtil.generateToken(client.getId(), user);
//        }
        getTokenStore().insert(token);
        
        return redirect;
    }

	private void authorize(Client client, OauthUser user, OAuthSession template)
			throws AuthorizationStoreException, IOException {
	    if (template.isOpenID() && user.getOpenIDAttributes().get(JwtUtil.OPENID_SUB) == null) {
	        setOpenidAttributes(user, template);
            
        }
		AuthorizationCode authorization = getAuthorizationStore().createAuthorizationCode(client, user,
				template.getScopes(), template.getRedirectUri(), AUTHORIZATION_TOKEN_EXPIRE_TIMESTAMP);
		authorization.setIsOpenID(template.isOpenID());
		
		getAuthorizationStore().insert(authorization);
		String redirectURI = authorization.getRedirectURI();
		if (redirectURI.endsWith("?")) 
		    redirectURI = redirectURI.substring(0,  redirectURI.length()-1); // strip trailing ?
		
		String redirect = String.format("%s?code=%s", redirectURI, authorization.getCode());
		if (template.getState() != null)
			redirect += "&state=" + URLEncoder.encode(template.getState(), "UTF-8");

		clearError();
		clearInfo();
		getRequest().getSession().invalidate();
		logger.debug("Redirecting user {} for clientid {} to {}", user.getUsername(), client.getId(), redirect);
		getResponse().sendRedirect(redirect);
	}

    private void setOpenidAttributes(OauthUser user, OAuthSession template) {
        String personid = null;
        user.setOpenIDAttribute(JwtUtil.OPENID_EMAIL, user.getUsername());
        user.setOpenIDAttribute(JwtUtil.OPENID_EMAIL_VERIFIED, true);
        OauthUserIdentity identity = null;
        if ( user.getIdentities().size() > 0) {
        	    identity = user.getIdentities().iterator().next();
            personid = identity.getPersonId();
            user.setOpenIDAttribute(JwtUtil.OPENID_NAME, identity.getName());
            user.setOpenIDAttribute(JwtUtil.OPENID_GIVENNAME, identity.getFirstName());
            user.setOpenIDAttribute(JwtUtil.OPENID_FAMILYNAME, identity.getLastName());
            user.setOpenIDAttribute(JwtUtil.OPENID_GENDER, identity.getGender());
            user.setOpenIDAttribute(JwtUtil.OPENID_PERSONID, identity.getPersonId());
            user.setOpenIDAttribute(JwtUtil.OPENID_PICTURE, identity.getImageUrl());
        }
        String sub = user.getPublicUserId();
        if (personid != null) {
            sub += personid;
        }
        user.setOpenIDAttribute(JwtUtil.OPENID_SUB, sub);
        
        String application = (String) template.getClient().getAttributes().get("APPLICATION");
        if ("VOETBALTV".equals(application)) {
            // ugh
            user.setOpenIDAttribute(JwtUtil.OPENID_NICKNAME, personid);
            user.setOpenIDAttribute(JwtUtil.OPENID_PREF_USERNAME, sub);
            
            // double ugh
            if (identity != null && identity.getInfix() != null) {
                user.setOpenIDAttribute(JwtUtil.OPENID_FAMILYNAME, identity.getInfix() + " " + identity.getLastName());
            }
        }
        
        
        if (template.getNonce() != null && !"".equals(template.getNonce().trim())) {
            user.setOpenIDAttribute(JwtUtil.OPENID_NONCE, template.getNonce());
        }
    }
}
