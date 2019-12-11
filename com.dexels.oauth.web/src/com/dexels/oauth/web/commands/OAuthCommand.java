package com.dexels.oauth.web.commands;

import java.io.IOException;

import javax.servlet.ServletException;

import com.dexels.oauth.web.exceptions.OAuthInvalidRequestException;

public interface OAuthCommand {
    public void execute() throws IOException, ServletException, OAuthInvalidRequestException;
}
