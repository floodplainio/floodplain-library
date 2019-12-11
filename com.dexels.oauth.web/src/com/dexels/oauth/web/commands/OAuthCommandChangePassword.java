package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.exception.PasswordResetException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandChangePassword extends OAuthCommandBase implements OAuthCommand {

	public static Integer MIN_PASSWORD_LENGTH = 8;
	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandChangePassword.class);
	
	public OAuthCommandChangePassword(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		String password = getRequestParameter("password");
		Integer userId;
		try {
		    userId = Integer.valueOf(getRequestParameter("userId"));
		} catch (NumberFormatException e) {
		    throw new ServletException("Invalid userid");
		}
		
		String activationId = getRequestParameter("activationId");
		
		boolean isSuccessful = false;
		String title = "Error";
		String message = "Incorrecte url gebruikt voor wachtwoord wijzigen";
		
		if (userId != null && activationId != null) {
			if (password == null || password.length() < MIN_PASSWORD_LENGTH) {
				message = "Wachtwoord te kort, minimaal " + MIN_PASSWORD_LENGTH + " tekens";
			} else {
				try {
					boolean isValidLink = getPasswordResetter().isValidActivationId(userId, activationId);	
					
					if (isValidLink) {
						isSuccessful = getPasswordResetter().savePassword(password, userId, activationId);
						
						if (isSuccessful) {
							title = "Success";
							message = "Uw wachtwoord is veranderd";
						}
					} else {
						message = "Link is verlopen";
					}
				} catch (PasswordResetException e) {
					logger.info("Password reset exception {}", e);
				}
			}
		}
		
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode(); 

		rootNode.put("isSuccessful", isSuccessful);
		rootNode.put("title", title);
		rootNode.put("message", message);
		
		getResponse().setContentType("application/json");
		mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);	
	}
}
