package com.dexels.oauth.web.commands;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthCommandFactory {
	
	private static final  Logger logger = LoggerFactory.getLogger(OAuthCommandFactory.class);
	
	private static final String ENDPOINT_LOGIN_STATUS = "/loginstatus";
	private static final String ENDPOINT_LOGIN = "/loginendpoint";
	private static final String ENDPOINT_LOGOUT = "/logout";
	private static final String ENDPOINT_TOKEN = "/token";
	private static final String ENDPOINT_FORGOT = "/forgot";
	private static final String ENDPOINT_REGISTER = "/register";
	private static final String ENDPOINT_CHANGE = "/changeendpoint";
	private static final String ENDPOINT_LINK = "/linkendpoint";
	private static final String END_POINT_CLIENT_DESCRIPTION = "/descriptionendpoint";
	private static final String END_POINT_SCOPE_ACCEPTANCE = "/scopeacceptanceeendpoint";
	private static final String ENDPOINT_OPENID = "/openid";
	
	public static OAuthCommand create (String path, HttpServletRequest request, HttpServletResponse response) {
		logger.debug("Receive path: {}", path);
		
		if (path == null) {
			return new OAuthCommandInitialize(request, response);
		} else if (ENDPOINT_LOGIN_STATUS.equals(path)) {
			return new OAuthCommandLoginStatus(request, response);
		} else if (ENDPOINT_LOGIN.equals(path)) {
			return new OAuthCommandLogin(request, response);
		} else if (ENDPOINT_TOKEN.equals(path)) {
			return new OAuthCommandGrantType(request, response);
		} else if (ENDPOINT_FORGOT.equals(path)) {
			return new OAuthCommandResetPassword(request, response);
		} else if (ENDPOINT_REGISTER.equals(path)) {
            return new OAuthCommandRegisterAccount(request, response);
		} else if (ENDPOINT_CHANGE.equals(path)) {
			return new OAuthCommandChangePassword(request, response);
		} else if (ENDPOINT_LINK.equals(path)) {
            return new OAuthCommandLink(request, response);
		} else if (ENDPOINT_LOGOUT.equals(path)) {
			return new OAuthCommandLogout(request, response);
		} else if (END_POINT_CLIENT_DESCRIPTION.equals(path)) {
			return new OAuthCommandDescription(request, response);
		} else if (END_POINT_SCOPE_ACCEPTANCE.equals(path)) {
			return new OAuthCommandScopeAcceptance(request, response);
		}else if (path.startsWith(ENDPOINT_OPENID)) {
            return new OauthCommandOpenID(request, response);
        }
		
		return new OAuthUnknownPath(request, response);
	}
}
