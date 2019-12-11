package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dexels.oauth.api.ScopeAcceptanceStore;
import com.dexels.oauth.api.ScopeAcceptanceStoreFactory;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;

public class OAuthCommandScopeAcceptance extends OAuthCommandBase implements OAuthCommand {
	public OAuthCommandScopeAcceptance(HttpServletRequest request, HttpServletResponse response) {
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

		if (template.getUser() == null || template.getClient() == null) {
			setError("", "Gebruiker incorrect");
			sendToLogin(template);
			return;
		}

		ScopeAcceptanceStore scopeAccStore = ScopeAcceptanceStoreFactory.getInstance();
		if (scopeAccStore != null) {
			scopeAccStore.setAcceptedScopesFor(template.getUser(), template.getClient(), getTemplate().getScopes());
		}

		new OAuthCommandPostLogin(getRequest(), getResponse()).execute();
	}
}
