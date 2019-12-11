package com.dexels.sharedconfigstore;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.sharedconfigstore.api.SharedConfigurationUpdateListener;

@Component(name = "dexels.configstore.configcreator", immediate = true)
public class OsgiConfigCreator implements SharedConfigurationUpdateListener {
    private final static Logger logger = LoggerFactory.getLogger(OsgiConfigCreator.class);
    private ConfigurationAdmin configAdmin;

    @Override
    public void handleConfigurationUpdate(String pid, Map<String, String> settings) {
        logger.info("Going to create configuration for: {}", pid);
        try {
            Configuration newConfig = configAdmin.getConfiguration(pid);
            Dictionary<String, Object> d = newConfig.getProperties();
            if (d == null) {
                d = new Hashtable<String, Object>();
            }

            for (String key : settings.keySet()) {

                d.put(key, settings.get(settings));
            }
            newConfig.update(d);

        } catch (IOException e) {
           logger.error("Exception on creating configuration: {}", e);
        }
    }

    @Reference(name = "ConfigAdmin", unbind = "clearConfigAdmin")
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = configAdmin;
    }

    public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = null;
    }

}
