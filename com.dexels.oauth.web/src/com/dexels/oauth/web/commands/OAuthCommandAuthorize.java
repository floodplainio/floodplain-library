package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.AuthorizationCode;
import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.RefreshToken;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.exception.AuthorizationStoreException;
import com.dexels.oauth.api.exception.StoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.exceptions.OAuthException;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;
import com.dexels.oauth.web.exceptions.OAuthServerErrorException;
import com.dexels.oauth.web.util.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nimbusds.jose.util.Base64;

public class OAuthCommandAuthorize extends OAuthCommandBase implements OAuthCommand {
    private final static Logger logger = LoggerFactory.getLogger(OAuthCommandAuthorize.class);

    public OAuthCommandAuthorize(HttpServletRequest request, HttpServletResponse response) {
        super(request, response);
    }

    @Override
    public void execute() throws IOException, ServletException {
   
        
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        getResponse().setContentType("application/json");

        String code = getRequestParameter("code");
        String redirectURI = getRequestParameter("redirect_uri");
        
        String clientId = getRequestParameter("client_id");
        String secret = null;
        if (clientId == null) {
            // Try taking from header 
            String header = getRequest().getHeader("Authorization");
            if (header != null && header.contains("Basic "))  {
                logger.debug("Using auth header for client/secret");
                String basicAuthString = header.split("Basic ")[1];
                String decoded = new Base64(basicAuthString).decodeToString();
                if (decoded.contains(":")) {
                    clientId = decoded.substring(0,  decoded.indexOf(":"));
                    secret = decoded.substring(decoded.indexOf(":")+1);
                } else {
                    logger.warn("Invalid basic auth header {}", header);
                }
            }
        } else {
            secret = getRequestParameter("client_secret");
            if (secret == null ) {
                secret = getRequestParameter("secret");
            }
        }

        try {
            if (code == null)
                throw new OAuthInvalidRequestException("Missing code parameter");

            if (redirectURI == null)
                throw new OAuthInvalidRequestException("Missing redirect uri");

            if (clientId == null)
                throw new OAuthInvalidRequestException("Missing client id");
            
            if (secret == null)
                throw new OAuthInvalidRequestException("Missing secret");

            logger.debug("Received authorization request for client {} code {}", clientId, code);
            
            Client client = getClient(clientId);
            if (client == null) {
                throw new OAuthInvalidRequestException("Invalid client");
            }
            
            AuthorizationCode authorization = getCode(code, clientId, secret);
            
            if (authorization == null) {
                throw new OAuthInvalidRequestException("Invalid code");
            }

            if (!authorization.getRedirectURI().equals(redirectURI)) {
                logger.warn("Invalid redirect uri: got {} but expected {}", redirectURI, authorization.getRedirectURI());
                throw new OAuthInvalidRequestException("Not the same host");

            }

            if (authorization.isExpired()) {
                logger.warn("Refusing an expired auth code");
                try {
                    getAuthorizationStore().delete(authorization);
                } catch (AuthorizationStoreException e) {
                }
                throw new OAuthInvalidRequestException("Invalid code");
            }

           
            Long refreshTokenExpires = REFRESH_TOKEN_EXPIRE_TIMESTAMP;
           
            if (client.getAttributes().containsKey("REFRESH_TOKEN_EXPIRES")) {
                refreshTokenExpires = (Long) client.getAttributes().get("REFRESH_TOKEN_EXPIRES");
            }
            OAuthToken token = getTokenStore().createOAuthToken(authorization, TOKEN_TYPE, TOKEN_EXPIRE_TIMESTAMP);
            RefreshToken refreshToken = getRefreshTokenStore().createRefreshToken(token, refreshTokenExpires);

            ArrayNode node = mapper.createArrayNode();
            for (Scope scope : token.getScopes()) {
                node.add(scope.getId());
            }

            rootNode.put("access_token", token.getCode());
            rootNode.put("token_type", token.getTokenType());
            rootNode.put("expires_in", TOKEN_EXPIRE_TIMESTAMP / 1000);
            rootNode.put("refresh_token", refreshToken.getCode());
            rootNode.set("scope", node);
            
            if (authorization.isOpenID()) {
                authorization.setIsOpenID(true);
                rootNode.put("id_token", JwtUtil.generateToken(token, authorization.getUser()));
            }

            try {
                getTokenStore().insert(token);
                getRefreshTokenStore().insert(refreshToken);
            } catch (StoreException e) {
                throw new OAuthServerErrorException("Store exception");
            }

            logger.info("Succesful code request for user {} clientId {}", token.getUsername(), clientId);
            try {
                getAuthorizationStore().delete(authorization);
            } catch (AuthorizationStoreException e) {
                // Not pretty but we can live with this
            }
        } catch (OAuthException e) {
            logger.error("Error in auth flow!", e);
            rootNode.put("error", e.getTitle());
            rootNode.put("error_description", e.getMessage());
            getResponse().setStatus(400);
        }
        mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
    }

//    private void printRequest(HttpServletRequest req) {
//        Enumeration<String> requestParameters = req.getParameterNames();
//        while (requestParameters.hasMoreElements()) {
//            String paramName = (String) requestParameters.nextElement();
//            logger.info("Request Parameter Name:  {} value {}", paramName,  req.getParameter(paramName));
//        }
//        
//        Enumeration<String> requestAttributes = req.getHeaderNames();
//        while (requestAttributes.hasMoreElements()) {
//            String attributeName = (String) requestAttributes.nextElement();
//            logger.info("Request header Name: {} value {}",  attributeName, req.getHeader(attributeName));
//        }
//    }
    
}
