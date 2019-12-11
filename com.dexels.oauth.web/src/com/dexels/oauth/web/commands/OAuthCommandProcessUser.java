package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.OauthUserIdentity;
import com.dexels.oauth.api.ScopeAcceptanceStore;
import com.dexels.oauth.api.ScopeAcceptanceStoreFactory;
import com.dexels.oauth.api.UserAuthenticatorFactory;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;

public class OAuthCommandProcessUser extends OAuthCommandBase implements OAuthCommand {

    private final static Logger logger = LoggerFactory.getLogger(OAuthCommandProcessUser.class);
    public final static long AUTHORIZATION_TOKEN_EXPIRE_TIMESTAMP = 600000;

    public OAuthCommandProcessUser(HttpServletRequest request, HttpServletResponse response) {
        super(request, response);
    }

    @Override
    public void execute() throws IOException, ServletException, OAuthInvalidRequestException {
        OAuthSession template = getTemplate();
        if (template == null) {
			sendToSessionExpired();
			return;
		}
        
        Client client = template.getClient();
        OauthUser user = template.getUser();
        if (user != null) {
            Object selectIdentity = client.getAttributes().get("SELECT_IDENTITY");

            if (selectIdentity != null && Boolean.valueOf(selectIdentity.toString())) {
                if (user.getIdentities().size() == 1) {
                    // Auto link
                    OauthUserIdentity userIdentity = user.getIdentities().iterator().next();
                    try {
                        UserAuthenticatorFactory.getInstance().linkUser(user, userIdentity.getPersonId(), userIdentity.getDomain(), client);
                    } catch (Exception e) {
                        logger.error("Exception on auto linking {} to {}", user.getUsername(), userIdentity.getPersonId(), e);
                    }
                } else if (user.getIdentities().size() > 1) {
                    // User has to pick an identity
                    logger.debug("Need to confirm identity, userid {} clientid: {}", user.getUserId(), client.getId());
                    sendToLinkIdentity(template);
                    return;
                }
            }

            // The user is valid, but before we can proceed to make some kind of token
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
        } else {
            logger.warn("Error in flow - user should not be null at this point!");
        }

        setError("Inloggen niet mogelijk", "onjuiste inloggegevens");
        sendToLogin(template);
    }

}
