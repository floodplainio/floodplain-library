package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dexels.oauth.web.OAuthCommandBase;

public class OAuthUnknownPath extends OAuthCommandBase implements OAuthCommand {
	
	public OAuthUnknownPath(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
	public void execute() throws IOException {
		getResponse().sendError(404);
	}
}
