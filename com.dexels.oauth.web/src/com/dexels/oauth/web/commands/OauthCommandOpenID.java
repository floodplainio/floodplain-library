package com.dexels.oauth.web.commands;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.OAuthToken;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.api.ScopeStoreFactory;
import com.dexels.oauth.api.exception.StoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.util.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.nimbusds.jose.jwk.RSAKey;

public class OauthCommandOpenID extends OAuthCommandBase implements OAuthCommand {
    private static final Logger logger = LoggerFactory.getLogger(OauthCommandOpenID.class);

    private static final String MODE_CERTS = "certs";
    private static final String MODE_USER = "user";

    public static final List<String> PROFILE_CLAIMS = Arrays.asList("name", "family_name", "given_name", "middle_name", "nickname",
            "preferred_username", "profile", "picture", "website", "gender", "birthdate", "zoneinfo", "locale", "updated_at");
    public static final List<String> TOKEN_PROFILE_CLAIMS = Arrays.asList("name", "picture");
    public static final List<String> EMAIL_CLAIMS = Arrays.asList("email", "email_verified");
    private String mode = null;

    public OauthCommandOpenID(HttpServletRequest request, HttpServletResponse response) {
        super(request, response);

        String path = request.getPathInfo().substring(8);
        if (MODE_USER.equals(path)) {
            mode = MODE_USER;
        } else if (MODE_CERTS.equals(path)) {
            mode = MODE_CERTS;
        } else {
            // unsupported
        }

    }

    @Override
    public void execute() throws IOException, ServletException {
        if (mode == null)
            return;

        if (mode == MODE_CERTS) {

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode rootNode = mapper.createObjectNode();

            ArrayNode keys = mapper.createArrayNode();
            rootNode.set("keys", keys);

            RSAKey openidKey = JwtUtil.getOpenidKey();

            ObjectNode keyNode = mapper.createObjectNode();
            keyNode.put("kty", openidKey.getKeyType().toString());
            keyNode.put("alg", openidKey.getAlgorithm().toString());
            keyNode.put("use", openidKey.getKeyUse().toString());

            keyNode.put("kid", openidKey.getKeyID());
            keyNode.put("n", openidKey.getModulus().toString());
            keyNode.put("e", openidKey.getPublicExponent().toString());

            keys.add(keyNode);

            getResponse().setHeader("content-type", "application/json; charset=UTF-8");
            mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
        } else if (mode == MODE_USER) {

            try {
                OAuthToken token = getTokenFromHeader(getRequest().getHeader("Authorization"));

                if (token == null) {
                    logger.info("UserInfo requested for unknown token. Auth header {}", getRequest().getHeader("Authorization"));
                    // Unauthorized
                    getResponse().setStatus(401);
                    return;
                }
                logger.info("UserInfo requested for {}", token.getUsername());

                Scope openIDScope = ScopeStoreFactory.getInstance().getScope("openid");
                if (!token.getScopes().contains(openIDScope)) {
                    logger.warn("Userinfo requested for {} (client {} but no openid scope! Scopes: {}", token.getUsername(),
                            token.getClientId(), token.getScopes());
                    getResponse().setStatus(401);
                    return;
                }
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode rootNode = mapper.createObjectNode();

                rootNode.put(JwtUtil.OPENID_SUB, (String) token.getUser().getOpenIDAttributes().get(JwtUtil.OPENID_SUB));
                Set<String> claims = new HashSet<>();
                for (Scope s : token.getScopes()) {
                    if (s.getId().equals("profile")) {
                        claims.addAll(PROFILE_CLAIMS);
                        claims.add(JwtUtil.OPENID_PERSONID);
                    }
                    if (s.getId().equals("email")) {
                        claims.addAll(EMAIL_CLAIMS);
                    }
                }
                for (String claim : claims) {
                    if (token.getUser().getOpenIDAttributes().containsKey(claim)) {
                        rootNode.set(claim, new POJONode(token.getUser().getOpenIDAttributes().get(claim)));
                    }
                }

                getResponse().setHeader("content-type", "application/json; charset=UTF-8");
                mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);

            } catch (StoreException e) {
                logger.error("Error in showing UserInfo", e);
                getResponse().setStatus(500);
            }

        }

    }

    private OAuthToken getTokenFromHeader(String authHeader) throws StoreException {
        if (authHeader == null || authHeader.equals("")) {
            return null;
        }
        if (!authHeader.startsWith("Bearer ")) {
            return null;
        }

        String tokenString = authHeader.split("Bearer ")[1];
        OAuthToken token = getTokenStore().getToken(tokenString);
        return token;

    }

}
