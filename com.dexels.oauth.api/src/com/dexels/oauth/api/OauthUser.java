package com.dexels.oauth.api;

import java.util.List;
import java.util.Map;

public interface OauthUser {
    public Integer getUserId();
    
    public String getPublicUserId();

    public String getUsername();
    
    public String getUnion();
    
    public void setAttribute(String key, Object value);
    
    public void unsetAttribute(String key);
   
    public Map<String, Object> getAttributes();
    
    public void setOpenIDAttribute(String key, Object value);
       
    public Map<String, Object> getOpenIDAttributes();
    
    public void clearIdentities();
    
    public void addIdentity(OauthUserIdentity identity);
    
    public List<OauthUserIdentity> getIdentities();

   
}
