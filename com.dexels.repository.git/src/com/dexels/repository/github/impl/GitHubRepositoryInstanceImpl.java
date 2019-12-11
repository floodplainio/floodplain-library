package com.dexels.repository.github.impl;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.client.ClientProtocolException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.repository.git.impl.GitRepositoryInstanceImpl;
import com.dexels.repository.github.GitHubRepositoryInstance;
import com.dexels.server.mgmt.api.ServerHealthProvider;

@Component(name = "dexels.repository.github.repository",  configurationPolicy = ConfigurationPolicy.REQUIRE, immediate = true, property = { "repo=git","event.topics=github/change" }, service = {
        GitHubRepositoryInstance.class, RepositoryInstance.class, EventHandler.class })
public class GitHubRepositoryInstanceImpl extends GitRepositoryInstanceImpl implements GitHubRepositoryInstance, EventHandler {
    private final static Logger logger = LoggerFactory.getLogger(GitHubRepositoryInstanceImpl.class);

    private String httpUrl;
    private String token;
    private String reponame;
    private String callbackUrl;
    private Integer callbackUrlId;

    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Override
    @Activate
    public void activate(Map<String, Object> settings) throws IOException, InvalidRemoteException, TransportException,
            GitAPIException {

        gitUrl = (String) settings.get("url");
        reponame =GithubUtils.extractGitHubRepoNameFromuri(gitUrl);
        httpUrl = GithubUtils.extractHttpUriFromGitUri(gitUrl,reponame);
        if (settings.containsKey("httpUrl")) {
            httpUrl = (String) settings.get("httpUrl"); // overwrite
        }

        if (settings.containsKey("token")) {
            token = (String) settings.get("token");
        }
        if(token==null) {
        	logger.info("Fallback to env token");
        	token = System.getenv("GIT_REPOSITORY_TOKEN");
        }
        if(token!=null) {
        	logger.info("Fallback worked.");
        }

        this.type = (String) settings.get("repository.type");
        this.callbackUrl =getCallBackUrl(settings);
        
        try {
			if (this.callbackUrl!=null) {
			    this.callbackUrlId = GithubUtils.registerCallbackUrl(this.reponame,this.callbackUrl,this.token,GithubUtils.extractUserFromGitHubURI(gitUrl),"json");
			}
		} catch (Throwable e) {
			logger.error("Registering github hook failed, continuing:", e);
		}
        super.activate(settings);

        if (this.callbackUrl == null && this.tag == null) {
        	initializeThread(settings);
        }

    }
    
    public static void main(String[] args) throws ClientProtocolException, IOException {
    	Integer ii = GithubUtils.registerCallbackUrl("repo", "git@github.com:org/repo", "xxxxxx", "Dexels","json");
    	System.err.println(">> "+ii);
    }

    @Deactivate
    public void deactivate() {
        if (callbackUrlId != null) {
            GithubUtils.deregisterCallbackUrl(reponame,callbackUrlId);
        }
    }
    
    private String getCallBackUrl(Map<String, Object> settings) {
    	String callBackURL = (String) settings.get("callbackUrl");
    	if(callBackURL!=null) {
    		return callBackURL;
    	}
    	return System.getenv("GIT_REPOSITORY_CALLBACKURL");
    	
    }
    @Override
    public synchronized void checkRefreshApplication() throws IOException {
        if (callbackUrl != null) {
            // do nothing - we get our changed from the callback
            return;
        }
        long now = System.currentTimeMillis();
        refreshApplication();
        long elapsed = System.currentTimeMillis() - now;
        logger.debug("Refresh app iteration took: " + elapsed + " millis.");
    }


    @Override
    protected UsernamePasswordCredentialsProvider getCredentialsProvider() {
        if (token == null || token.equals("")) {
            return super.getCredentialsProvider();
        }
        UsernamePasswordCredentialsProvider upc = new UsernamePasswordCredentialsProvider(token, "");
        return upc;
    }

    @Override
    public String getHttpUrl() {
        return httpUrl;
    }

    @Override
    public String getUrl() {
        return gitUrl;
    }

    @Override
    public String repositoryType() {
        return "github";
    }

    @Reference(name = "EventAdmin", unbind = "clearEventAdmin", cardinality=ReferenceCardinality.OPTIONAL)
    public void setEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = eventAdmin;
    }

    public void clearEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = null;
    }

    @Override
    @Reference(name = "ConfigAdmin", unbind = "clearConfigAdmin")
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        super.setConfigAdmin(configAdmin);
    }

    @Reference(name = "RepositoryManager", unbind = "clearRepositoryManager", policy=ReferencePolicy.DYNAMIC)
    public void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = repositoryManager;
    }

    public void clearRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = null;
    }
    
    @Reference(cardinality=ReferenceCardinality.MULTIPLE, unbind="removeOperation",policy=ReferencePolicy.DYNAMIC)
    @Override
    public void addOperation(AppStoreOperation op, Map<String, Object> settings) {
        super.addOperation(op, settings);
    }
    
    public void removeOperation(AppStoreOperation op, Map<String, Object> settings) {
    	super.removeOperation(op, settings);
    }
    
    @Reference(name = "HazelcastService", unbind = "clearHazelcastService",cardinality=ReferenceCardinality.OPTIONAL, policy=ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService service) {
        this.hazelcastService = service;
    }

    public void clearHazelcastService(HazelcastService service) {
        this.hazelcastService = null;
    }
    
    
    @Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearServerHealthProvider", cardinality=ReferenceCardinality.OPTIONAL)
    public void setServerHealthProvider(ServerHealthProvider provider) {
       super.setServerHealthProvider(provider);
    }
    
    public void clearServerHealthProvider(ServerHealthProvider provider) {
        super.clearServerHealthProvider(provider);
    }


	@Override
	public void handleEvent(Event event) {
        if (event.getTopic().equals("github/change")) {
            if (this.gitUrl.equals(event.getProperty("url")) && this.branch.equals(event.getProperty("branch"))) {
                logger.info("Received push event in github repo. From: {} to: {} url: {} ref: {}", event.getProperty("before"), event.getProperty("head"),
                        event.getProperty("url"), event.getProperty("ref"));

                executorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            refreshApplicationLocking();
                        } catch (Throwable e) {
                            logger.error("Error: ", e);
                        }

                    }
                });

            } else {
                logger.info("Not responding to this event: {}", event);
            }
        }
	}


}
