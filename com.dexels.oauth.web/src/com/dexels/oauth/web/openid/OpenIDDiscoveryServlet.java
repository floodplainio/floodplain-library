package com.dexels.oauth.web.openid;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.web.util.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Component(name = "dexels.oauth.openidservlet", service = Servlet.class, immediate = true, property = { "alias=/.well-known",
        "servlet-name=openid-discovery" }, configurationPolicy = ConfigurationPolicy.OPTIONAL)
public class OpenIDDiscoveryServlet extends HttpServlet {

    private static final long serialVersionUID = -1948354354961917987L;

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if (!"/openid-configuration".equals(request.getPathInfo())) {
            // invalid request
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("issuer", JwtUtil.getIssuer());

        rootNode.put("authorization_endpoint", JwtUtil.getAuthEndpoint());
        rootNode.put("token_endpoint", JwtUtil.getTokenEndpoint());
        rootNode.put("userinfo_endpoint", JwtUtil.getUserInfoEndpoint());
        rootNode.put("jwks_uri", JwtUtil.getJwksUri());

        ArrayNode responseTypes = mapper.createArrayNode();
        responseTypes.add("code");
        rootNode.set("response_types_supported", responseTypes);

        ArrayNode subjectTypes = mapper.createArrayNode();
        subjectTypes.add("public");
        rootNode.set("subject_types_supported", subjectTypes);

        ArrayNode tokenSigningAlgs = mapper.createArrayNode();
        tokenSigningAlgs.add("RS256");
        rootNode.set("id_token_signing_alg_values_supported", tokenSigningAlgs);

        ArrayNode scopes = mapper.createArrayNode();
        scopes.add("openid");
        scopes.add("email");
        scopes.add("profile");
        rootNode.set("scopes_supported", scopes);

        ArrayNode displayValues = mapper.createArrayNode();
        displayValues.add("page");
        displayValues.add("popup");
        rootNode.set("display_values_supported", displayValues);

        ArrayNode claims = mapper.createArrayNode();
        claims.add("sub");
        claims.add("email");
        claims.add("email_verified");
        claims.add("name");
        claims.add("given_name");
        claims.add("family_name");
        claims.add("personid");
        claims.add("gender");
        rootNode.set("claims_supported", claims);


        response.setHeader("content-type", "application/json; charset=UTF-8");
        mapper.writerWithDefaultPrettyPrinter().writeValue(response.getWriter(), rootNode);
    }

}
