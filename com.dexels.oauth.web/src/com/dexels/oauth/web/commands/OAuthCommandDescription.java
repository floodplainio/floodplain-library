package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.exception.ClientStoreException;
import com.dexels.oauth.web.OAuthCommandBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandDescription extends OAuthCommandBase implements OAuthCommand {

	private final static Logger logger = LoggerFactory.getLogger(OAuthCommandDescription.class);
	
	public OAuthCommandDescription(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		String clientId = getRequestParameter("client_id");
		
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		
		try {
			if (clientId != null) {
				Client client = getClientStore().getClient(clientId);
				if (client != null)
					rootNode.put("description", client.getDescription());
			}
		} catch (ClientStoreException e) {
			logger.info(e.getMessage());
		}
		
		getResponse().setContentType("application/json");
		mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
	}
}
