package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.api.exception.PasswordResetException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandResetPassword extends OAuthCommandBase implements OAuthCommand {

	public OAuthCommandResetPassword(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		String username = getRequest().getParameter("username");
		OAuthSession template = getTemplate();
		
		if (template == null) {
			sendToSessionExpired();
			return;
		}
		
		boolean isSuccessful = false;
		String title = "";
		String message = "Geen account gevonden";
		
		String email = UserAuthenticatorFactory.getInstance().getEmail(username, template.getClient());
		if (email != null && email.length() != 0) {
			try {
				isSuccessful = getPasswordResetter().resetPassword(username, email, template.getClient());
				if (isSuccessful) {
					title = "Succes";
					message = "Er is een email met instructies om uw wachtwoord te herstellen verstuurd";
				}
			} catch (PasswordResetException e) {
				message = "Server fout, probeer het later nog eens";
			}
		} else {
			message = "Ongeldig e-mailadres ingevoerd";
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
