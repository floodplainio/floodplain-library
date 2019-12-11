package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.exceptions.OAuthInvalidGrantException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OAuthCommandInvalidGrantType extends OAuthCommandBase implements OAuthCommand {

	public OAuthCommandInvalidGrantType(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException, ServletException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		getResponse().setContentType("application/json");
		OAuthInvalidGrantException exception = new OAuthInvalidGrantException();
		rootNode.put("error", exception.getTitle());
		rootNode.put("error_description", exception.getMessage());
		getResponse().setStatus(400);
		mapper.writerWithDefaultPrettyPrinter().writeValue(getResponse().getWriter(), rootNode);
	}
}
