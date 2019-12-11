package com.dexels.sharedconfigstore.api;

import java.util.Map;

public interface SharedConfigurationUpdateListener {
    public void handleConfigurationUpdate(String key, Map<String, String> configMap);
}
