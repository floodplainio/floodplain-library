package com.dexels.oauth.web.openid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

@Component(name = "dexels.oauth.openid", service = OpenIDConnect.class, immediate = true)

public class OpenIDConnect {

    private ConfigurationAdmin configAdmin;
    private List<Configuration> myConfigs;

    @Activate
    public void activate() {
        myConfigs = new ArrayList<>();
        registerScopes();
    }

    private void registerScopes() {
        try {
            Configuration openIdConfig = configAdmin.createFactoryConfiguration("dexels.oauth.scope", null);
            Dictionary<String, Object> properties = new Hashtable<String, Object>();
            properties.put("scope.id", "openid");
            properties.put("scope.title", "openid");
            properties.put("scope.description", "openid");
            openIdConfig.update(properties);
            myConfigs.add(openIdConfig);

            openIdConfig = configAdmin.createFactoryConfiguration("dexels.oauth.scope", null);
            properties = new Hashtable<String, Object>();
            properties.put("scope.id", "profile");
            properties.put("scope.title", "Persoonsgegevens");
            properties.put("scope.description", "Persoonsgegevens zoals naam, geslacht, geboortedatum en foto");
            openIdConfig.update(properties);
            myConfigs.add(openIdConfig);

            openIdConfig = configAdmin.createFactoryConfiguration("dexels.oauth.scope", null);
            properties = new Hashtable<String, Object>();
            properties.put("scope.id", "email");
            properties.put("scope.title", "E-mail");
            properties.put("scope.description", "Uw e-mail adres");
            openIdConfig.update(properties);
            myConfigs.add(openIdConfig);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Deactivate
    public void deactivate() {
        for (Configuration c : myConfigs) {
            try {
                c.delete();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Reference(unbind = "clearConfigAdmin", policy = ReferencePolicy.DYNAMIC)
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = configAdmin;
    }


    public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = null;
    }
}
