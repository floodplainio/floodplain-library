package com.dexels.oauth.impl;

import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.oauth.api.Scope;

@Component(name="dexels.oauth.scope",configurationPolicy=ConfigurationPolicy.REQUIRE,immediate=true)
public class SimpleScope implements Scope {

	private String id;
	private String description;
	private boolean isOptional = true;
    private String title;
	
	@Activate
	public void activate(Map<String,Object> settings) {
		this.id = (String)settings.get("scope.id");
		this.title = (String)settings.get("scope.title");
		this.description = (String)settings.get("scope.description");
		if (settings.containsKey("scope.optional")) {
		    isOptional = Boolean.valueOf( (String)settings.get("scope.optional"));
		}
	}
	
	@Override
    public boolean equals(Object obj) {
	    if (obj instanceof Scope) {
	        Scope otherScope = (Scope) obj;
	        return this.getId().equals(otherScope.getId());
	    } 
	    return super.equals(obj);
	    
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
	public String getId() {
		return id;
	}

	@Override
	public String getDescription() {
		return description;
	}
	
	@Override
    public String getTitle() {
        return title;
    }

    @Override
    public boolean isOptional() {
        return isOptional;
    }

    @Override
    public boolean isOpenIDScope() {
        return id.equals("openid");
    }
}
