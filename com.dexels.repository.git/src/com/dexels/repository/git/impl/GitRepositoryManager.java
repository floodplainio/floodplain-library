package com.dexels.repository.git.impl;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "dexels.repository.git.env")
public class GitRepositoryManager {

    private final static Logger logger = LoggerFactory.getLogger(GitRepositoryManager.class);

    protected ConfigurationAdmin configAdmin;
    protected Configuration configuration;
    private Map<String, String> env;

    @Activate
    public void activate() throws IOException {
        env = System.getenv();

        injectStorageConfig();

        // Check some mandatory properties
        if (extractProperty("GIT_REPOSITORY_URL") == null) {
            logger.warn("No 'GIT_REPOSITORY_URL' set, so no git repositories will be injected.");
            return;
        }
        if (extractProperty("GIT_REPOSITORY_TYPE") == null) {
            logger.warn("No 'GIT_REPOSITORY_TYPE' set, so no git repositories will be injected.");
            return;
        }

        injectConfig();
        
    }

    @Deactivate
    public void deactivate() {
        try {
            if (configuration != null) {
                configuration.delete();
            }
        } catch (IOException e) {
            logger.error("Error: ", e);
        }
    }

    protected void injectConfig() throws IOException {
        Configuration c = null;

        String github = extractProperty("GIT_REPOSITORY_GITHUB");
        if (github != null && github.equals("true")) {
            c = createOrReuse("dexels.repository.github.repository", "(repository.name=system.managed.repository)");
        } else {
            c = createOrReuse("dexels.repository.git.repository", "(repository.name=system.managed.repository)");
        }

        Dictionary<String, Object> properties = new Hashtable<String, Object>();

        String url = extractProperty("GIT_REPOSITORY_URL");
        String branch = extractProperty("GIT_REPOSITORY_BRANCH");
        String commit = extractProperty("GIT_REPOSITORY_COMMIT");
        String tag = extractProperty("GIT_REPOSITORY_TAG");
        String deployment = extractProperty("GIT_REPOSITORY_DEPLOYMENT");
        String type = extractProperty("GIT_REPOSITORY_TYPE");
        String sleepTime = extractProperty("GIT_REPOSITORY_SLEEPTIME");
        String fileinstall = extractProperty("GIT_REPOSITORY_FILEINSTALL");
        String token = extractProperty("GIT_REPOSITORY_TOKEN");
        String callbackUrl = extractProperty("GIT_REPOSITORY_CALLBACKURL");

        // mandatory properties
        properties.put("repo", "git");
        properties.put("repository.type", type);
        properties.put("branch", branch);
        properties.put("name", "env.managed.repository");
        properties.put("url", url);

        // Optional properties
        if (sleepTime != null) {
            properties.put("sleepTime", sleepTime);
        }
        if (fileinstall != null) {
            properties.put("fileinstall", fileinstall);
        }
        if (deployment != null) {
            properties.put("repository.deployment", deployment);
        }
        if (token != null) {
            properties.put("token", token);
        }
        if (tag != null) {
            properties.put("tag", tag);
        }
        if (commit != null) {
            properties.put("commit", commit);
        }
        if (callbackUrl != null) {
            properties.put("callbackUrl", callbackUrl);
        }

        c.update(properties);


    }

	private void injectStorageConfig() throws IOException {
		String storagePath = extractProperty("GIT_REPOSITORY_STORAGE");
        if (storagePath != null && !"".equals(storagePath)) {
            Configuration manager = configAdmin.getConfiguration("navajo.repository.manager", null);
            Dictionary<String, Object> managerProperties = new Hashtable<String, Object>();
            managerProperties.put("storage.path", storagePath);
            String tempPath = extractProperty("GIT_REPOSITORY_TEMP");
            if (tempPath != null && !"".equals(tempPath)) {
                managerProperties.put("storage.temp", tempPath);
            }
            String outputPath = extractProperty("GIT_REPOSITORY_OUTPUT");
            if (outputPath != null && !"".equals(outputPath)) {
                managerProperties.put("storage.output", outputPath);
            }

            manager.update(managerProperties);
        } else {
            logger.info("No 'GIT_REPOSITORY_STORAGE' set, so no navajo.repository.manager configuration will be injected.");

        }
	}

    protected Configuration createOrReuse(String pid, final String filter) throws IOException {
        configuration = null;
        try {
            Configuration[] c = configAdmin.listConfigurations(filter);
            if (c != null && c.length > 1) {
                logger.warn("Multiple configurations found for filter: {}", filter);
            }
            if (c != null && c.length > 0) {
                configuration = c[0];
            }
        } catch (InvalidSyntaxException e) {
            logger.error("Error in filter: {}", filter, e);
        }
        if (configuration == null) {
            configuration = configAdmin.createFactoryConfiguration(pid, null);
        }
        return configuration;
    }

    @Reference(name = "ConfigAdmin", unbind = "clearConfigAdmin")
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = configAdmin;
    }

    /**
     * @param configAdmin
     *            the configAdmin to remove
     */
    public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = null;
    }

    protected String extractProperty(String key) {
        if (env.containsKey(key)) {
            return env.get(key);
        }
        return System.getProperty(key);
    }
}
