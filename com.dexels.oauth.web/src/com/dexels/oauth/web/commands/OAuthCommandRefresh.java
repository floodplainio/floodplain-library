package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.RefreshToken;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.api.exception.RefreshTokenStoreException;
import com.dexels.oauth.api.exception.StoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.exceptions.OAuthException;
import com.dexels.oauth.web.exceptions.OAuthInvalidGrantException;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;
import com.dexels.oauth.web.exceptions.OAuthServerErrorException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandRefresh extends OAuthCommandBase implements OAuthCommand {
    private final static Logger logger = LoggerFactory.getLogger(OAuthCommandRefresh.class);

	public OAuthCommandRefresh(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		String refreshTokenString = getRequestParameter("refresh_token");
		String clientId = getRequestParameter("client_id");
		String secret = getRequestParameter("client_secret");
        if (secret == null ) {
            secret = getRequestParameter("secret");
        }

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		getResponse().setContentType("application/json");
		try {
			
			if (refreshTokenString == null) 
				throw new OAuthInvalidRequestException("Invalid refresh code");
			
			if (clientId == null)
				throw new OAuthInvalidRequestException("Invalid client");
			
			RefreshToken refreshToken = null;
			try {
				refreshToken = getRefreshTokenStore().getRefreshToken(refreshTokenString, clientId, secret);
			} catch (RefreshTokenStoreException e1) {
				logger.error("RefreshTokenStoreException upon retrieving of refresh token {}",refreshTokenString, e1);
				throw new OAuthInvalidGrantException();
			}
			
			if (refreshToken == null) {
				throw new OAuthInvalidGrantException();
			} else if (refreshToken != null && refreshToken.isExpired()) {
				try {
					getRefreshTokenStore().delete(refreshToken);
				} catch (RefreshTokenStoreException e) {}
				throw new OAuthInvalidGrantException();
			}
			
			Long refreshTokenExpires = REFRESH_TOKEN_EXPIRE_TIMESTAMP;
            Client client = getClient(clientId);
            if (client == null) {
                throw new OAuthInvalidGrantException();
            }
            

            // Check if user still exists
            OauthUser user = UserAuthenticatorFactory.getInstance().getUser(refreshToken.getUsername(),null, client );
           
            // Check user
            if (user == null) {
                logger.warn("Unable to find user for refresh token {} username {} clientid {}. User has been deleted?", refreshTokenString,
                        refreshToken.getUsername(), client.getId());
                throw new OAuthInvalidGrantException();
            }
            if (! user.getUserId().equals(refreshToken.getUser().getUserId())) {
                logger.warn( "Found user with different userid. Refresh token {} token-userid {} found user-id {} clientid {}",
                        refreshTokenString, refreshToken.getUser().getUserId(), user.getUserId(), client.getId());
                throw new OAuthInvalidGrantException();
            }
            
            if (client.getAttributes().containsKey("REFRESH_TOKEN_EXPIRES")) {
            	    refreshTokenExpires = (Long) client.getAttributes().get("REFRESH_TOKEN_EXPIRES");
            }
            logger.info("Refreshed token for {} for clientId {} ({})", user.getUsername(), client.getId(), client.getDescription());

			
			OAuthToken token = getTokenStore().createOAuthToken(refreshToken, TOKEN_TYPE, TOKEN_EXPIRE_TIMESTAMP);
			RefreshToken newRefreshToken = getRefreshTokenStore().createRefreshToken(token, refreshTokenExpires);
			
			ArrayNode node = mapper.createArrayNode();
			for (Scope scope : token.getScopes()) {
				node.add(scope.getId()); 
			}
						
			rootNode.put("access_token", token.getCode());
			rootNode.put("token_type", token.getTokenType());
			rootNode.put("expires_in", TOKEN_EXPIRE_TIMESTAMP / 1000);
			rootNode.put("refresh_token", newRefreshToken.getCode());
			rootNode.set("scope", node);
			if (refreshToken.updateUser()) {
				rootNode.put("update_user", refreshToken.updateUser());
			}
			
			try {
				getTokenStore().insert(token);
				getRefreshTokenStore().insert(newRefreshToken);
			} catch (StoreException e) {
				throw new OAuthServerErrorException("Store exception");
			}
			
			//Clean up
			try {
				getTokenStore().delete(refreshToken.getOAuthTokenCode());
				getRefreshTokenStore().delete(refreshToken);
			} catch (StoreException e) {}
		} catch (OAuthException e) {
			rootNode.put("error", e.getTitle());
			rootNode.put("error_description", e.getMessage());
			getResponse().setStatus(400);
		}	
		mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
	}

}
