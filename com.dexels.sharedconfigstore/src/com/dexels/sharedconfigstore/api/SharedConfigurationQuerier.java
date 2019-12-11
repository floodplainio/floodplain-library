package com.dexels.sharedconfigstore.api;

import java.util.Set;

public interface SharedConfigurationQuerier {
    /** 
     * Retrieve a list of known containers
     */
    public Set<String> getContainers();
    
    /** 
     * 
     * @param container leave null for generic config
     */
    public String getConfiguration(String pid, String container);
}
