package com.dexels.oauth.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.Application;
import com.dexels.oauth.api.Client;
import com.dexels.oauth.api.Scope;

@Component(name="dexels.oauth.client.simple", configurationPolicy=ConfigurationPolicy.REQUIRE)
public class SimpleClient implements Client {
	
	private String id;
	private String secret;
	private String description;
	private String username;
	private String redirectURL;
	private String instance;
	private Set<String> allowedGrantTypes;
	private Map<String, Object> attributes;
	
	@Activate
	public void activate(Map<String,Object> settings) {
		id = (String) settings.get("client.id");
		secret = (String) settings.get("client.secret");
		description = (String) settings.get("client.description");
		username = (String) settings.get("client.username");
		redirectURL = (String) settings.get("client.redirecturl");
		instance = (String) settings.get("client.instance");

		
		attributes = new HashMap<>();
        attributes.put("SELECT_IDENTITIY", settings.get("client.selectidentity"));
		if (settings.containsKey("client."+Client.CONFIRM_SCOPES_ATTRIBUTEKEY)) {
            attributes.put(Client.CONFIRM_SCOPES_ATTRIBUTEKEY, Boolean.valueOf((String) settings.get("client."+Client.CONFIRM_SCOPES_ATTRIBUTEKEY)));
		}
		
		allowedGrantTypes = new HashSet<>();
		if (settings.containsKey("client.allowedGrantTypes")) {
		    String grantTypesString = (String) settings.get("client.allowedGrantTypes");
		    StringTokenizer st = new StringTokenizer(grantTypesString,  ",");
		    while (st.hasMoreTokens()) {
		        allowedGrantTypes.add(st.nextToken());
		    }
		}
	}
	
	public SimpleClient () {
		
	}
	
	public SimpleClient (String id, String secret, String description, String username, String redirectURL, String instance, Map<String, Object> attributes) { 
		this.id = id;
		this.description = description;
		this.username = username;
		this.redirectURL = redirectURL;
		this.instance = instance;
		this.secret = secret;
		
	    allowedGrantTypes = new HashSet<>();

	}
	
	@Override
	public Map<String, Object> getAttributes (){
		return attributes;
	}
	
	@Override
	public String getId() {
		return id;
	}
	
	@Override
	public String getUsername() {
		return username;
	}
	
	
	@Override
	public String getRedirectURL() {
		return redirectURL;
	}
	
	@Override
	public String getInstance() {
		return instance;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public String getSecret() {
		return secret;
	}

    @Override
    public Set<String> getAllowedGrantTypes() {
        return allowedGrantTypes;
    }

    @Override
    public Set<Scope> getDefaultScopes() {
        return new HashSet<>();
    }

    @Override
    public Set<Scope> getAllowedScopes() {
        return null;
    }

	@Override
	public Application getApplication() {
		return new Application() {
			
			@Override
			public boolean isSingleAccount() {
				return true;
			}
		};
	}
}