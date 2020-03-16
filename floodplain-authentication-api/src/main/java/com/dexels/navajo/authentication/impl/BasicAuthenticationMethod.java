package com.dexels.navajo.authentication.impl;

import com.dexels.navajo.authentication.api.AAAQuerier;
import com.dexels.navajo.authentication.api.AuthenticationMethod;
import com.dexels.navajo.script.api.Access;
import com.dexels.navajo.script.api.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.StringTokenizer;

public class BasicAuthenticationMethod implements AuthenticationMethod {
	private static final Logger logger = LoggerFactory.getLogger(BasicAuthenticationMethod.class);

	private AAAQuerier authenticator;

	private String username;
	private String password;

	public void setAAAQuerier(AAAQuerier aa) {
		authenticator = aa;
	}

	public void clearAAAQuerier(AAAQuerier aa) {
		authenticator = null;
	}

	@Override
	public String getIdentifier() {
		return BASIC_IDENTIFIER;
	}

	@Override
	public void process(Access access) throws AuthorizationException {
		access.rpcUser = username;
		access.rpcPwd = password;
		authenticator.process(access);
	}

	@Override
	public AuthenticationMethod getInstanceForRequest(String header) {
		BasicAuthenticationMethod newInstance = new BasicAuthenticationMethod();
		newInstance.getAuthenticationFromHeader(header);
		newInstance.setAAAQuerier(this.authenticator);
		return newInstance;

	}

	private void getAuthenticationFromHeader(String authHeader) {
		StringTokenizer st = new StringTokenizer(authHeader);
		if (st.hasMoreTokens() && st.nextToken().equalsIgnoreCase(getIdentifier())) {
			String credentials;
			credentials = new String(Base64.getDecoder().decode(st.nextToken()));

			int p = credentials.indexOf(':');
			if (p != -1) {
				this.username = credentials.substring(0, p).trim();
				this.password = credentials.substring(p + 1).trim();
				logger.debug("Successfully authenticated: {}", username);
			} else {
				logger.warn("Invalid authentication token: {}", credentials);
			}
		}

	}

}
