package com.dexels.sharedconfigstore.api;

import java.util.Map;

public interface SharedConfigurationUpdater {
    /** 
     * 
     * @param pid the PID of the configuration object
     * @param container optional a container for which this configuration is relevant. leave 
     * null to apply this to all containers
     * @param settings The configuration settings for this PID
     */
    public void createOrUpdateConfiguration(String pid, String container, Map<String, String> settings);
}
