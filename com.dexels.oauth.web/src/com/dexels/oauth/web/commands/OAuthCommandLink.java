package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.ScopeAcceptanceStore;
import com.dexels.oauth.api.ScopeAcceptanceStoreFactory;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;

public class OAuthCommandLink extends OAuthCommandBase implements OAuthCommand {

	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandLink.class);
	public final static long AUTHORIZATION_TOKEN_EXPIRE_TIMESTAMP = 600000;

	public OAuthCommandLink(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		clearError();
		clearInfo();

		OAuthSession template = getTemplate();
		if (template == null) {
			sendToSessionExpired();
			return;
		}

		String personid = getRequestParameter("personid");
		String domain = getRequestParameter("domain");
		OauthUser user = template.getUser();
		Client client = template.getClient();

		if (client != null && user != null) {
			try {
				UserAuthenticatorFactory.getInstance().linkUser(user, personid, domain, client);

				// The user is valid and linked, but before we can proceed to make some kind of
				// token
				// the user needs to explicitly tell us he will accept the requested/default
				// scopes
				if (template.isConfirmScopes() && !template.getScopes().isEmpty()) {
					boolean accepted = false;
					if (template.getScopes().size() == 1 && template.getScopes().iterator().next().isOpenIDScope()) {
						accepted = true;
					} else {
						ScopeAcceptanceStore scopeAccStore = ScopeAcceptanceStoreFactory.getInstance();
						if (scopeAccStore != null) {
							// Check if the user previously accepted these scopes already
							accepted = scopeAccStore.hasAcceptedScopesFor(user, client, template.getScopes());
						}
					}

					if (!accepted) {
						logger.debug("Need to confirm scopes, id: {}", client.getId());
						sendToAuthorize(template);
						return;
					}
				}

				OAuthCommand command = new OAuthCommandPostLogin(getRequest(), getResponse());
				command.execute();
				return;
			} catch (Exception e) {
				logger.error("Exception in linking user: {}", user.getUsername(), e);
			}
		} else {
			logger.warn("Client id is null or user is missing");
		}

		logger.warn("Failed link for {}", user.getUsername());
		setError("", "Gebruiker incorrect");
		sendToLogin(template);
	}
}
