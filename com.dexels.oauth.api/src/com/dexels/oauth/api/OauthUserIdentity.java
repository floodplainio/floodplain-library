package com.dexels.oauth.api;

public interface OauthUserIdentity {
    public String getPersonId();
    public void setPersonId(String personid);
    
    public String getDomain();
    public void setDomain(String domain);
    
    public String getName();
    public void setName(String name);
    
    public String getFirstName();
    public void setFirstName(String name);
    
    public String getInfix();
    public void setInfix(String infix);
    
    public String getLastName();
    public void setLastName(String lastname);
    
    public String getGender();
    public void setGender(String gender);

    public String getImageUrl();
    public void setImage(String url);
}
