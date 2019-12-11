package com.dexels.oauth.web.commands;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.SSOToken;
import com.dexels.oauth.api.SSOTokenStoreFactory;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.api.exception.InvalidSWTTokenException;
import com.dexels.oauth.api.exception.TokenStoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.dexels.oauth.web.exceptions.OAuthException;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;
import com.dexels.oauth.web.exceptions.OAuthInvalidScopeException;
import com.dexels.oauth.web.exceptions.OAuthLoginRequiredException;
import com.dexels.oauth.web.exceptions.OAuthUnsupportedResponseTypeException;
import com.dexels.oauth.web.util.CookieUtil;
import com.dexels.oauth.web.util.JwtUtil;

public class OAuthCommandInitialize extends OAuthCommandBase implements OAuthCommand {
	
	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandInitialize.class);
	
	public OAuthCommandInitialize(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
    public void execute() throws ServletException, IOException, OAuthInvalidRequestException {
		String redirectURI = getRequestParameter("redirect_uri");
		String clientId = getRequestParameter("client_id");
		String responseType = getRequestParameter("response_type");
		String scopesString = getRequestParameter("scope");
		String state = getRequestParameter("state");
		String loginPageType = getRequestParameter("login_page_type"); // Deprecated! Use Display parameter
		String display = getRequestParameter("display");
		String prompt = getRequestParameter("prompt");
		String nonce = getRequestParameter("nonce");
		
        if (clientId == null)
            throw new OAuthInvalidRequestException("Missing clientId parameter");

        if (redirectURI == null)
            throw new OAuthInvalidRequestException("Missing redirect uri");

        // Without the redirect uri, we cannot do anything since we cannot respond back.
        // So just give up.
		logger.info("Initializing, received: redirect_uri: {}, client_id: {}, response_type: {}, scope: {}, state: {}", redirectURI, clientId, responseType, scopesString, state);
		

		try {
			Client client = getClient(clientId);
			
            if (client == null)
				throw new OAuthInvalidRequestException("Invalid client id");
			
			logger.info("Initializing client, id: {} instance:{} club: {}", client.getId(), client.getInstance(), client.getAttributes());
			boolean openid = false;
			if (scopesString == null)  scopesString = ""; // unusual, but hey
					
			Set<Scope>scopes = new HashSet<>();
			if (!scopesString.trim().equals("")) {
			    String splitter = " ";
			    if (scopesString.contains(",")) {
			        splitter = ",";
			    }
			    
			    for (String scopeString : scopesString.split(splitter)) {
                    if (scopeString.equals("openid")) {
                        openid = true;
                    } 
                    Scope scope = getScope(scopeString);
                    if (scope != null) {
                        scopes.add(scope);
                    } else {
                        throw new OAuthInvalidScopeException();
                    }
                }
			}
		   
		    if (client.getAllowedScopes() != null) {
		        Set<Scope> toRemove = new HashSet<>();
		        for (Scope requestedScope : scopes) {
		            if (!client.getAllowedScopes().contains(requestedScope) && !requestedScope.isOpenIDScope()) {
		                toRemove.add(requestedScope);
		            }
		        }
		        scopes.removeAll(toRemove);
		    }
		    
			if (!"code".equals(responseType) && !"token".equals(responseType)) 
				throw new OAuthUnsupportedResponseTypeException();
			
			setTemplate(new OAuthSession(client, scopes, redirectURI, state, responseType));
			getTemplate().setDefaultScopes(client.getDefaultScopes());
			getTemplate().setIsOpenID(openid);
			
			if (openid) {
			    getTemplate().setNonce(nonce);
			    
			    // The following parameters are not (yet) supported
			    String max_age = getRequestParameter("max_age");
			    if (max_age != null) logger.info("Ignoring max_age {}", max_age);
			    
			    String ui_locales = getRequestParameter("ui_locales");
			    if (ui_locales != null) logger.info("Ignoring ui_locales {}", ui_locales);
			    
			    String id_token_hint = getRequestParameter("id_token_hint");
			    if (id_token_hint != null) logger.info("Ignoring id_token_hint {}", id_token_hint);
			    
			    String login_hint = getRequestParameter("login_hint");
			    if (login_hint != null) logger.info("Ignoring login_hint {}", login_hint);
			    
			    String acr_values = getRequestParameter("acr_values");
			    if (acr_values != null) logger.info("Ignoring acr_values {}", acr_values);
			    
			    String claims = getRequestParameter("claims");
                if (claims != null) logger.info("Ignoring claims {}", claims);
			}
			
			clearError();
			
			OAuthCommand cmd = handleSSO(responseType, scopesString, prompt, client);
			if (cmd != null) {
			    cmd.execute();
			    return;
			}
            
            sendToLogin(getTemplate());
		} catch (OAuthException exception) {
			report(redirectURI, exception);
		} 
		
	}

    private OAuthCommand handleSSO(String responseType, String scopesString, String prompt, Client client)
            throws OAuthLoginRequiredException {
        
        
        String ssoCookieCode = CookieUtil.getValue(getRequest(), NAVAJOSESSION_COOKIE);
       
        OauthUser user = null;
        if (ssoCookieCode != null) {

            // Check if we still have this code and if it's not expired
            try {
                SSOToken ssoToken = SSOTokenStoreFactory.getInstance().getSSOToken(ssoCookieCode);
                if (ssoToken == null || ssoToken.isExpired()) {
                    logger.warn("Unable to find sso token in the store - ignoring");
                    throw new InvalidSWTTokenException();
                }
    
                String navajosession = JwtUtil.getSSOSubject(ssoCookieCode);
                
                String[] splitted = navajosession.split(":");
                if (splitted.length != 2) {
                    throw new InvalidSWTTokenException();
                }
                String username = splitted[0];
                int userId = Integer.valueOf(splitted[1]);

                if (!ssoToken.getUsername().equals(username) || ssoToken.getUserId() != userId) {
                    logger.error("This should never happen - username-userid mismatch! cookie {} username1: {} userid1: {}, username2: {} userid2: {}", 
                            ssoCookieCode, username, userId, ssoToken.getUsername(), ssoToken.getUserId() );
                    throw new InvalidSWTTokenException();
                }
                user = UserAuthenticatorFactory.getInstance().getUser(username, client);
                if (user != null) {
                    logger.info("Found sso user {} for cientid {}", user.getUsername(), client.getId());
                }
                
            } catch (TokenStoreException e1) {
                logger.error("Exception checking for SSO token", e1);
            } catch (InvalidSWTTokenException | NumberFormatException  e) {
                logger.warn("Invalid sso token! {}", ssoCookieCode, e);
            }
        }
        
         // Check prompt
        if (responseType.equals("code") && "none".equals(prompt) && user == null) {
            // if prompt equals "none" and we don't have an oauth user, we should throw an expection
            logger.info("No user found and response type=none, hence throw OAuthLoginRequiredException");
            throw new OAuthLoginRequiredException();
        }
         
        OAuthCommand command = null;
        if (user != null && !"login".equals(prompt)) {
            logger.info("Using sso flow for {}", user.getUsername());
            getTemplate().setUser(user);
            getTemplate().setIsSSOLogin(true);
            command = new OAuthCommandProcessUser(getRequest(), getResponse());
        }
        return command;
    }

	private void report (String uri, OAuthException exception) throws IOException {
		String redirect = String.format("%s?error=%s&error_description=%s", uri, exception.getTitle(), exception.getMessage());
		getResponse().setStatus(400);
		getResponse().sendRedirect(redirect);
	}
}
