package com.dexels.oauth.impl;

import java.util.Date;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.SSOToken;

@Component(name = "dexels.oauth.token.simple-sso", configurationPolicy = ConfigurationPolicy.OPTIONAL, immediate=true)
public class SimpleSSOToken implements SSOToken {
    
    private long createdAt;
    private long expireAt;
    private String username;
    private int userId;
    private String code;
    private String clientId;
    private String ipAddress;
    private String userAgent;

    public SimpleSSOToken() {
        super();
    }
    
    public SimpleSSOToken(String clientId, OauthUser user, long expireAt, String code) {
        this.createdAt = System.currentTimeMillis();
        this.clientId = clientId;
        this.username = user.getUsername();
        this.userId = user.getUserId();
        this.expireAt = expireAt;
        this.code = code;
    }

    @Override
    public Date getCreatedAt() {
        return new Date(createdAt);
    }


    @Override
    public boolean isExpired() {
        return expireAt > new Date().getTime();
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public int getUserId() {
        return userId;
    }


    @Override
    public String getCode() {
        return code;
    }


    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void setIpAddress(String ip) {
        this.ipAddress = ip;
        
    }

    @Override
    public String getIpAddress() {
        return ipAddress;
    }

    @Override
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
        
    }

    @Override
    public String getUserAgent() {
        return userAgent;
    }

}
