package com.dexels.oauth.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.OauthUserIdentity;

@Component(name = "dexels.oauth.user.simple", configurationPolicy = ConfigurationPolicy.REQUIRE)
public class SimpleUser implements OauthUser {
    private Integer userid;
    Map<String, Object> attributes = new HashMap<>();
    Map<String, Object> openIdAttributes = new HashMap<>();

    @Activate
    public void activate(Map<String, Object> settings) {
        userid = Integer.valueOf((String) settings.get("user.userid"));

        attributes.put("USERNAME", settings.get("user.username"));
        attributes.put("UNION", settings.get("user.union"));
        attributes.put("PUBLICUSERID", settings.get("user.publicuserid"));
    }

    public SimpleUser() {

    }


    @Override
    public Integer getUserId() {
        return userid;
    }


    @Override
    public String getPublicUserId() {
        return (String) attributes.get("PUBLICUSERID");
    }

    @Override
    public String getUsername() {
        return (String) attributes.get("USERNAME");
    }

    @Override
    public String getUnion() {
        return (String) attributes.get("UNION");
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
        
    }

    @Override
    public void unsetAttribute(String key) {
        attributes.remove(key);
    }
    
	@Override
	public void setOpenIDAttribute(String key, Object value) {
		openIdAttributes.put(key, value);
		
	}

	@Override
	public Map<String, Object> getOpenIDAttributes() {
		return openIdAttributes;
	}

    @Override
    public void clearIdentities() {

        
    }

    @Override
    public void addIdentity(OauthUserIdentity identity) {


    }

    @Override
    public List<OauthUserIdentity> getIdentities() {
        return new ArrayList<>();
    }



}
