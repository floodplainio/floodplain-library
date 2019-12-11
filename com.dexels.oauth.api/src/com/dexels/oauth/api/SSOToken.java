package com.dexels.oauth.api;

import java.util.Date;

public interface SSOToken {

    /* Basic fields - these should always be present */
    public Date getCreatedAt();
    public String getCode();
	public String getUsername();
	public int getUserId();
	public String getClientId();
    public boolean isExpired();

    /* Additional fields */
	public void setIpAddress(String ip);
    public String getIpAddress();
    
    public void setUserAgent(String userAgent);
    public String getUserAgent();
    

}
