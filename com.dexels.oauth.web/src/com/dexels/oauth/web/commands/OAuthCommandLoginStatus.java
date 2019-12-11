package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OauthUserIdentity;
import com.dexels.oauth.api.Scope;
import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.OAuthSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandLoginStatus extends OAuthCommandBase implements OAuthCommand {

	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandLoginStatus.class);

	public OAuthCommandLoginStatus(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();

		OAuthSession template = getTemplate();
		if (template == null) {
			sendToSessionExpired();
			return;
		}

		Client client = template.getClient();

		HttpSession session = getRequest().getSession();
		String error = (String) session.getAttribute("error");
		String errorheader = (String) session.getAttribute("errorheader");
		String infoheader = (String) session.getAttribute("infoheader");

		if (error != null)
			rootNode.put("error", error);

		if (errorheader != null)
			rootNode.put("errorheader", errorheader);
		
		if (infoheader != null)
			rootNode.put("infoheader", infoheader);

		if (client != null)
			rootNode.put("clientdescription", client.getDescription());

		clearError();
		clearInfo();
		
		ArrayNode scopes = mapper.createArrayNode();
		for (Scope scope : template.getScopes()) {
			if (scope.isOpenIDScope()) {
				continue;
			}
			ObjectNode node = mapper.createObjectNode();
			node.put("id", scope.getId());
			node.put("title", scope.getTitle());
			node.put("description", scope.getDescription());
			node.put("optional", scope.isOptional());
			scopes.add(node);
		}
		rootNode.set("scopes", scopes);

		if (template.getUser() != null) {
			ArrayNode persons = mapper.createArrayNode();
			for (OauthUserIdentity identity : template.getUser().getIdentities()) {
				ObjectNode node = mapper.createObjectNode();
				node.put("personid", identity.getPersonId());
				node.put("firstname", identity.getFirstName());
				node.put("infix", identity.getInfix());
				node.put("lastname", identity.getLastName());
				node.put("name", identity.getName());
				if (identity.getImageUrl() != null) {
					node.put("image", identity.getImageUrl());
				}

				persons.add(node);
			}
			rootNode.set("persons", persons);
		}

		getResponse().setContentType("application/json");
		mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
	}
}
