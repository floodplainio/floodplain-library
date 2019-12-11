package com.dexels.oauth.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.OauthUser;
import com.dexels.oauth.api.UserAuthenticator;
import com.dexels.oauth.api.UserAuthenticatorFactory;

@Component(name="dexels.oauth.userauthenticator.simple",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class ServiceUserAuthenticator implements UserAuthenticator {
	
	private final Map<String, OauthUser> users = new HashMap<String, OauthUser>();
	
	@Activate
	public void activate () {
		UserAuthenticatorFactory.setInstance(this);
	}
	
    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeUser", cardinality=ReferenceCardinality.MULTIPLE)
    public void addUser(OauthUser user) {    	
        users.put(user.getUsername() + "-" + user.getUnion(), user);        
    }

    public void removeUser(OauthUser user) {
        users.remove(user.getUsername() + "-" + user.getUnion());
    }
    
	@Override
	public OauthUser getUser(String username, String password, Client client) {
		return users.get(username + "-" + client.getInstance());
	}
	

    @Override
    public OauthUser getUser(String username, Client client) {
        return users.get(username + "-" + client.getInstance());
    }

	@Override
	public String getEmail(String username, Client client) {
		return "stefan@example.com";
	}

    @Override
    public void linkUser(OauthUser user, String personid, String domain, Client client) throws Exception {
        // not implemented
        
    }

    @Override
    public void registerUser(String username, String password, Client client) throws Exception {
        OauthUser u = new SimpleUser();
        Map<String, Object> settings = new HashMap<>();
        settings.put("user.userid", (new Random()).nextInt(50000));
        settings.put("user.username", username);
        settings.put("user.union", "KNVB");

        users.put(username + "-" + "KNVB", u);
        
    }

    @Override
    public boolean userExists(String username, Client client) {
        for (OauthUser user : users.values()) {
            if (user.getUsername().equals(username)) {
                return true;
            }
        }
        return false;
    }


}