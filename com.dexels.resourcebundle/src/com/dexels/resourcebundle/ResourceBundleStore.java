package com.dexels.resourcebundle;

import java.io.IOException;

public interface ResourceBundleStore {
    public String getValidationDescription(String key, String union, String locale);
    
    public String getResource(String application, String resource, String union, String subunion, String locale) throws IOException;
}
