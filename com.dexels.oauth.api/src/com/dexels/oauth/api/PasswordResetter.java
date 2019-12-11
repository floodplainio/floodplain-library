package com.dexels.oauth.api;

import com.dexels.oauth.api.exception.PasswordResetException;

public interface PasswordResetter {
    public boolean isValidActivationId(Integer userId, String activationId) throws PasswordResetException;

    public boolean resetPassword(String username, String email, Client client) throws PasswordResetException;

    public boolean savePassword(String password, Integer userId, String activationId) throws PasswordResetException;
}