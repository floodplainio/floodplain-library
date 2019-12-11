package com.dexels.oauth.web.commands;

import java.io.IOException;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.RefreshToken;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.api.exception.RefreshTokenStoreException;
import com.dexels.oauth.api.exception.TokenStoreException;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.exceptions.OAuthException;
import com.dexels.oauth.web.exceptions.OAuthInvalidGrantException;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandPassword extends OAuthCommandBase implements OAuthCommand {
    private final static Logger logger = LoggerFactory.getLogger(OAuthCommandPassword.class);

    public OAuthCommandPassword(HttpServletRequest request, HttpServletResponse response) {
        super(request, response);
    }

    @Override
    public void execute() throws IOException, ServletException {
        String errorTitle = null;
        String errorMsg = null;

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        getResponse().setContentType("application/json");

        String username = getRequestParameter("username");
        String password = getRequestParameter("password");
        String clientId = getRequestParameter("client_id");

        try {
            if (username == null || username.trim().equals(""))
                throw new OAuthInvalidRequestException("Missing username");
            if (password == null || password.trim().equals(""))
                throw new OAuthInvalidRequestException("Missing password");
            if (clientId == null || clientId.trim().equals(""))
                throw new OAuthInvalidRequestException("Invalid clientId");

            Client client = getClient(clientId);
            if (client != null) {
                if (!client.getAllowedGrantTypes().isEmpty()) {
                    if (!client.getAllowedGrantTypes().contains(OAuthCommandGrantType.GRANT_TYPE_PASSWORD)) {
                        logger.warn("Client {} is not allowed to call Password grant type. Allowed is: {}", client.getId(), client.getAllowedGrantTypes());
                        throw new OAuthInvalidGrantException();
                    }
                }
                logger.debug("Client, id: {} instance:{}", client.getId(), client.getInstance(), client.getAttributes());

                OauthUser user = UserAuthenticatorFactory.getInstance().getUser(username, password, client);

                if (user == null) {
                    logger.warn("Incorrect username: {} for client {} ({})", username, clientId, client.getDescription());
                    errorTitle = "Inloggen niet mogelijk";
                    errorMsg = "onjuiste inloggegevens";
                } else {
                    logger.info("Password flow login for {} for clientId {} ({})", username, clientId, client.getDescription());
                    Set<Scope> scopes = client.getDefaultScopes();
                    OAuthToken token = getTokenStore().createOAuthToken(client.getId(), user, scopes, TOKEN_TYPE, TOKEN_EXPIRE_TIMESTAMP);
                    if (user.getUsername() != null) {
                        token.setUsername(username);
                    }

                    getTokenStore().insert(token);

                    Long refreshTokenExpires = REFRESH_TOKEN_EXPIRE_TIMESTAMP;
                    if (client.getAttributes().containsKey("REFRESH_TOKEN_EXPIRES")) {
                        refreshTokenExpires = (Long) client.getAttributes().get("REFRESH_TOKEN_EXPIRES");
                    }
                    RefreshToken refreshToken = getRefreshTokenStore().createRefreshToken(token, refreshTokenExpires);

                    try {
                        getRefreshTokenStore().insert(refreshToken);
                    } catch (RefreshTokenStoreException e) {
                        // not bad enough to let the whole flow crash...
                        logger.warn("RefreshTokenException ", e);
                    }

                    rootNode.put("access_token", token.getCode());
                    rootNode.put("token_type", token.getTokenType());
                    rootNode.put("expires_in", TOKEN_EXPIRE_TIMESTAMP / 1000);
                    rootNode.put("refresh_token", refreshToken.getCode());

                    mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);

                    return;

                }
            } else {
                logger.warn("Client {} not found", clientId);
                errorTitle = "";
                errorMsg = "Authenticatie fout";

            }

        } catch (OAuthException | TokenStoreException e) {
            logger.error("Oauth exception", e);
            errorTitle = "";
            errorMsg = "Server tijdelijk niet beschikbaar, probeer het later nog eens";
        }

        // We should only reach this part if something went wrong!
        logger.warn("Failed login for {}", username);
        getResponse().setStatus(400);

        rootNode.put("error", errorTitle);
        rootNode.put("description", errorMsg);
        mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
    }
}
