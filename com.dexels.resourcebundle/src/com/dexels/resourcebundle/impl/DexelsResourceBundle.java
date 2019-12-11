package com.dexels.resourcebundle.impl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.util.RepositoryEventParser;
import com.dexels.resourcebundle.ResourceBundleStore;

@Component(name = "dexels.resourcebundle", service = {ResourceBundleStore.class, EventHandler.class }, enabled = true, property = { "event.topics=repository/change" })
public class DexelsResourceBundle implements ResourceBundleStore, EventHandler {
    private static final String RESOURCES_FOLDER = "resources" + File.separator + "texts";
    private static final int MAX_RESOURCE_LENGTH = 29;
    private static final String PROPERTIES = ".properties";
    
    private final static Logger logger = LoggerFactory.getLogger(DexelsResourceBundle.class);

    private final Map<String, RepositoryInstance> applications = new HashMap<>();
    private final Map<String, String> cache = new HashMap<>();

    @Override
    public String getValidationDescription(String key, String union, String locale) {
        try {
            String props = getResource(null, "validation", union, null, locale);
            InputStream stream = new ByteArrayInputStream(props.getBytes(StandardCharsets.UTF_8));
            InputStreamReader isr = new InputStreamReader(stream,Charset.forName("UTF-8"));
            PropertyResourceBundle p = new PropertyResourceBundle(isr);
            return p.getString(key);
        } catch (MissingResourceException mre) {
            logger.info("Missing vaidation description: {} in validations for {} {}", key, union, locale);
        } catch (Throwable t) {
             logger.warn("Exception on getting validation description for {} {} {}", key, union, locale,t);
        }
        return null;
                
    }
    
       
    public String getResource(String application, String resource, String union, String subunion, String locale) throws IOException {
        PropertyResourceBundle baseprb = null;
        PropertyResourceBundle baseprbloc = null;
        PropertyResourceBundle unionprb = null;
        PropertyResourceBundle unionprbloc = null;
        PropertyResourceBundle subunionprb = null;
        PropertyResourceBundle subunionprbloc = null;

        if (resource == null || resource.trim().equals("")) {
            resource = "validation";
        } else {
            if (resource.contains("..") || resource.contains("/") || resource.length() > MAX_RESOURCE_LENGTH) {
                throw new IllegalArgumentException("Unsupported resource");
            }
        }
        
        String lookupKey = getLookupKey(application, resource, union, subunion, locale);
        if (cache.containsKey(lookupKey)) {
            return cache.get(lookupKey);
        }
        logger.debug("Cache-miss for {} ", lookupKey);

        String repoPath = null;

        if (applications.size() < 0) {
            logger.info("No applications bound!");
            return "";
        }

        if (application != null) {
            if (!applications.containsKey(application)) {
                logger.warn("Requested unknown application: {}", application);
                return "";
            }
            repoPath = applications.get(application).getRepositoryFolder().getAbsolutePath();
        } else {
            // Just take the first application we can find!
            repoPath = applications.values().iterator().next().getRepositoryFolder().getAbsolutePath();
        }

        String basepath = repoPath + File.separator + RESOURCES_FOLDER;

        File f = getResourceBundleFile(basepath, resource);
        if (f != null) {
            baseprb = readBundle(f);
        }

        f = getResourceBundleFile(basepath, locale, resource);
        if (f != null) {
            baseprbloc = readBundle(f);
        }

        if (union != null) {
            String unionpath = basepath + File.separator + union.toUpperCase();
            f = getResourceBundleFile(unionpath, resource);
            if (f != null) {
                unionprb = readBundle(f);
            }

            f = getResourceBundleFile(unionpath, locale, resource);
            if (f != null) {
                unionprbloc = readBundle(f);
            }
        }

        if (subunion != null && union != null) {
            String subunionpath = basepath + File.separator + union.toUpperCase() + File.separator + subunion.toUpperCase();
            f = getResourceBundleFile(subunionpath, resource);
            if (f != null) {
                subunionprb = readBundle(f);
            }

            f = getResourceBundleFile(subunionpath, locale, resource);
            if (f != null) {
                subunionprbloc = readBundle(f);
            }
        }

        // Combine the results
        Map<String, String> result = new HashMap<>();
        if (baseprb != null) {
            for (String key : baseprb.keySet()) {
                result.put(key, baseprb.getString(key));
            }
        }
        if (baseprbloc != null) {
            // Override with localized results
            for (String key : baseprbloc.keySet()) {
                result.put(key, baseprbloc.getString(key));
            }
        }
        if (unionprb != null) {
            // Override with union results
            for (String key : unionprb.keySet()) {
                result.put(key, unionprb.getString(key));
            }
        }
        if (unionprbloc != null) {
            // Override with localized union results
            for (String key : unionprbloc.keySet()) {
                result.put(key, unionprbloc.getString(key));
            }
        }
        if (subunionprb != null) {
            // Override with subunion results
            for (String key : subunionprb.keySet()) {
                result.put(key, subunionprb.getString(key));
            }
        }
        if (subunionprbloc != null) {
            // Override with localized subunion results
            for (String key : subunionprbloc.keySet()) {
                result.put(key, subunionprbloc.getString(key));
            }
        }

        StringBuilder builder = new StringBuilder();

        for (Entry<String, String> entry : result.entrySet()) {
            builder.append(entry.getKey());
            builder.append(" = ");
            builder.append(entry.getValue().replace("\n", "\\n"));
            builder.append("\n");
        }

        cache.put(lookupKey, builder.toString());
        return builder.toString();
    }

    private PropertyResourceBundle readBundle(File f) throws FileNotFoundException, IOException {
    	if(f==null || !f.exists()) {
    		return null;
    	}
    	try(FileInputStream fis = new FileInputStream(f)) {
    		InputStreamReader isr = new InputStreamReader(fis,Charset.forName("UTF-8"));
    		return new PropertyResourceBundle(isr);
    	}
    }
    private String getLookupKey(String application, String resource, String union, String subunion, String locale) {
        String key = "";
        if (resource != null) {
            key += resource;
        }
        if (application != null) {
            key += application;
        }
        if (union != null) {
            key += union;
        }
        if (subunion != null) {
            key += subunion;
        }
        if (locale != null) {
            key += locale;
        }
        return key;
    }

    @Reference(cardinality = ReferenceCardinality.MULTIPLE, unbind = "removeRepositoryInstance", policy = ReferencePolicy.DYNAMIC)
    public void addRepositoryInstance(RepositoryInstance a) {
        applications.put(a.getRepositoryName(), a);
    }

    public void removeRepositoryInstance(RepositoryInstance a) {
        applications.remove(a.getRepositoryName());
    }


    private File getResourceBundleFile(String basepath, String resource) {
        File f = new File(basepath, resource + PROPERTIES);
        if (f.exists()) {
            return f;
        }
        return null;
    }
    
    
    private File getResourceBundleFile(String basepath, String locale, String resource) {
        if (locale == null) {
            return null;
        }
        
        File f = new File(basepath, resource + "_" + locale.toLowerCase() + ".properties");
        if (f.exists()) {
            return f;
        }
        return null;
    }


    @Override
    public void handleEvent(Event e) {
        List<String> deletedContent = RepositoryEventParser.filterDeleted(e, RESOURCES_FOLDER);
        List<String> changedContent = RepositoryEventParser.filterChanged(e, RESOURCES_FOLDER);
        if (deletedContent.size() > 0 || changedContent.size() > 0) {
            logger.debug("Detected a change in the resources folder - clearing cache");
            cache.clear();
        }
    }
}
