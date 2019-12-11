package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandRegisterAccount extends OAuthCommandBase implements OAuthCommand {
    private static final Logger logger = LoggerFactory.getLogger(OAuthCommandRegisterAccount.class);

    public OAuthCommandRegisterAccount(HttpServletRequest request, HttpServletResponse response) {
        super(request, response);
    }

    @Override
    public void execute() throws IOException, ServletException {
        String username = getRequest().getParameter("username");
        String password = getRequest().getParameter("password");
        OAuthSession template = getTemplate();
        
        if (template == null) {
			sendToSessionExpired();
			return;
		}

        String title = "";
        String message = "Er is een fout opgetreden bij het registreren van uw account.";
        
        Client client = template.getClient();
        try {
            // First try getEmail, to see if this user exists already
            boolean exists = UserAuthenticatorFactory.getInstance().userExists(username, client);

            if (exists) {
                title = "";
                message = "Er bestaat al een account met dit e-mailadres";
            } else {
                UserAuthenticatorFactory.getInstance().registerUser(username, password, client);
                setInfo("", "Er is een e-mail naar u verzonden met een activatielink. Na activatie kunt u inloggen");
                sendToLogin(template);
                return;
            }

        } catch (Exception e) {
            logger.error("Exception on registring account for {}", username, e);
        }
        
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();

        rootNode.put("isSuccessful", false);
        rootNode.put("title", title);
        rootNode.put("message", message);

        getResponse().setContentType("application/json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
    }
}
