package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;

public class OAuthCommandLogin extends OAuthCommandBase implements OAuthCommand {

	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandLogin.class);
	public final static long AUTHORIZATION_TOKEN_EXPIRE_TIMESTAMP = 600000;

	public OAuthCommandLogin(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException, OAuthInvalidRequestException {
		clearError();
		clearInfo();

		OAuthSession template = getTemplate();
		if (template == null) {
			sendToSessionExpired();
			return;
		}

		String username = getRequestParameter("username");
		String password = getRequestParameter("password");
		String keepSession = getRequestParameter("keepSession");

		template.setKeepSession("on".equals(keepSession));

		Client client = template.getClient();

		if (client != null) {
			logger.info("Client, id: {}", client.getId());

			OauthUser user = UserAuthenticatorFactory.getInstance().getUser(username, password, client);
			if (user != null) {
				logger.info("{} flow login for {} for client {} ({})", template.getType(), username, client.getId(),
						client.getDescription());
				// Remember the logged in user for this session.
				template.setUser(user);
				OAuthCommand command = new OAuthCommandProcessUser(getRequest(), getResponse());
				command.execute();
				return;
			} else {
				logger.warn("Incorrect username: {}", username);
			}
		} else {
			logger.warn("Client id is null");
		}

		logger.warn("Failed login for {}", username);
		setError("Inloggen niet mogelijk", "onjuiste inloggegevens");
		sendToLogin(template);
	}
}
