package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.dexels.oauth.web.OAuthCommandBase;
import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;

public class OAuthCommandGrantType extends OAuthCommandBase implements OAuthCommand {

	public static String GRANT_TYPE_AUTHORIZATION = "authorization_code";
	public static String GRANT_TYPE_REFRESH = "refresh_token";
	public static String GRANT_TYPE_PASSWORD = "password";
	
	public OAuthCommandGrantType(HttpServletRequest request, HttpServletResponse response) {
		super(request, response);
	}

	@Override
    public void execute() throws IOException, ServletException, OAuthInvalidRequestException {
		OAuthCommandGrantType.create(getRequestParameter("grant_type"), getRequest(), getResponse()).execute();
	}
	
	public static OAuthCommand create (String grantType, HttpServletRequest request, HttpServletResponse response) {
		if (GRANT_TYPE_AUTHORIZATION.equals(grantType))
			return new OAuthCommandAuthorize(request, response);
		else if (GRANT_TYPE_REFRESH.equals(grantType))
			return new OAuthCommandRefresh(request, response);
		else if (GRANT_TYPE_PASSWORD.equals(grantType))
            return new OAuthCommandPassword(request, response);
		return new OAuthCommandInvalidGrantType(request, response);
	}
}
