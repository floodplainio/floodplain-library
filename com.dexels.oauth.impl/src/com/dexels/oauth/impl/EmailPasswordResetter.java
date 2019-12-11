package com.dexels.oauth.impl;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.PasswordResetter;
import com.dexels.oauth.api.PasswordResetterFactory;
import com.dexels.oauth.api.exception.PasswordResetException;

@Component(name="dexels.oauth.email.passwordresetter",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class EmailPasswordResetter implements PasswordResetter {

	@Activate
	public void activate () {
		PasswordResetterFactory.setInstance(this);
	}

	@Override
	public boolean isValidActivationId(Integer userId, String activationId) throws PasswordResetException {
		return true;
	}

	@Override
	public boolean resetPassword(String username, String email, Client client) throws PasswordResetException {
		return true;
	}

	@Override
	public boolean savePassword(String password, Integer userId, String activationId) throws PasswordResetException {
		return true;
	}	
}