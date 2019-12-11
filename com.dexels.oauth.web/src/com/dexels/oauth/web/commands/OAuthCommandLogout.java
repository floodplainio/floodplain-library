package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.exception.TokenStoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandLogout extends OAuthCommandBase  implements OAuthCommand {

	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandLogout.class);

	
	public OAuthCommandLogout(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		String stringToken = getRequestParameter("token");
		boolean success = false;

		try {
            OAuthToken token = getTokenStore().getToken(stringToken);
			if (token != null) {
                getTokenStore().delete(token.getCode());
                getRefreshTokenStore().delete(token);
				success = true;
				logger.info("Logout for token: {} username {}", stringToken, token.getUsername());

                // Delete all sso tokens from this user
                getSSOTokenStore().getSSOTokens(token.getUser()).forEach(ssoToken -> {
                    try {
                        getSSOTokenStore().deleteSSOToken(ssoToken.getCode());
                    } catch (TokenStoreException e) {
                        logger.error("Error deleting sso tokens", e);
                    }
                });
			}
        } catch (Exception e) {
			logger.error("Error logging out", e);
		}
	
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode(); 

		rootNode.put( "success", success );
		
		getResponse().addHeader("Access-Control-Allow-Origin", "*");
		getResponse().setContentType("application/json");
		
		if (success) {
			getResponse().setStatus(200);
		} else {
			getResponse().setStatus(400);
		}
		
		mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
	}
}
