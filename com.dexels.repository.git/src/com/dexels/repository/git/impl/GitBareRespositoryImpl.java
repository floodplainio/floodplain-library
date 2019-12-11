package com.dexels.repository.git.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.repository.git.GitRepository;
import com.dexels.repository.github.impl.GithubUtils;

@Component(name="dexels.repository.git.bare", configurationPolicy=ConfigurationPolicy.REQUIRE,immediate=true,property={"event.topics=repository/change","event.topics=github/change","bare=true"})
public class GitBareRespositoryImpl implements GitRepository, EventHandler {

	private RepositoryManager repositoryManager;
	private String repositoryName;
	private File gitFolder;
	private RevCommit lastCommit;
    protected Git git;
	private String gitUrl;
	private String token;
//	private Object callbackUrlId;
	private EventAdmin eventAdmin;
	
	private final static Logger logger = LoggerFactory.getLogger(GitBareRespositoryImpl.class);

	@Activate
	public void activate(Map <String,Object> settings) {
        try {
			File gitRepoFolder = repositoryManager.getRepositoryFolder();
			if (settings.containsKey("token")) {
			    token = (String) settings.get("token");
			}

			gitUrl = (String) settings.get("url");

			repositoryName = determineRepositoryName(settings);
			gitFolder = new File(gitRepoFolder, repositoryName);

			if (gitFolder.exists()) {
			    Repository repository = getRepository(gitFolder);
			    git = new Git(repository);
			    
			    try {
			        Iterable<RevCommit> log = git.log().call();
			        lastCommit = log.iterator().next();
			        repository.close();
			    } catch (NoHeadException e) {
			        // there was no head. That usually means a clone failure in a
			        // previous run. Delete and retry. Maybe also use for other failures.
			    	logger.trace("error",e);
			        FileUtils.deleteDirectory(gitFolder);
			        callClone();
			    }
			} else {
			    callClone();
			}
//			this.callbackUrlId = getCallBackUrl(settings);
            String callbackUrl = getCallBackUrl(settings);
	        if (callbackUrl!=null) {
	            String repoNameFromuri = GithubUtils.extractGitHubRepoNameFromuri(gitUrl);
				String githubUser = GithubUtils.extractUserFromGitHubURI(gitUrl);
				int callbackId = GithubUtils.registerCallbackUrl(repoNameFromuri,callbackUrl,this.token,githubUser,"json");
				logger.info("Registered git hook: "+callbackId);
	        }

		} catch (InvalidRemoteException e) {
			logger.error("Error: ", e);
		} catch (TransportException e) {
			logger.error("Error: ", e);
		} catch (IOException e) {
			logger.error("Error: ", e);
		} catch (GitAPIException e) {
			logger.error("Error: ", e);
		} catch (Throwable t) {
			logger.error("Error: ", t);
		}
        
     
	}
    
    private String getCallBackUrl(Map<String, Object> settings) {
    	String callBackURL = (String) settings.get("callbackUrl");
    	if(callBackURL!=null) {
    		return callBackURL;
    	}
    	return System.getenv("GITHUB_CALLBACK_URL");
    	
    }
    
    @Deactivate
	public void deactivate() {
		logger.info(">>>>>>>\n Deactivating GitRepo <<<<<<<");
	}
	@Override
	public File getGitFolder() {
		return gitFolder;
	}

    @Reference(name = "RepositoryManager", unbind = "clearRepositoryManager")
    public void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = repositoryManager;
    }

    public void clearRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = null;
    }
    
	private String determineRepositoryName(Map<String, Object> settings) {
        String settingsRepoName = (String) settings.get("repositoryname");
        if(settingsRepoName!=null) {
        	settingsRepoName = settingsRepoName.trim();
        }
        if(settingsRepoName!=null) {
            return settingsRepoName;
        }
        return GithubUtils.extractGitHubRepoNameFromuri(gitUrl);
	}
	

    private Repository getRepository(File basePath) throws IOException {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        Repository repository = builder.setGitDir(basePath).readEnvironment().findGitDir().build();
        
        return repository;
    }
    
    public void callClone() throws GitAPIException, InvalidRemoteException, TransportException, IOException {

        Repository repository = null;
        try {
            repository = getRepository(gitFolder);
            git = Git.cloneRepository().setProgressMonitor(new LoggingProgressMonitor()).setBare(true)
                    .setCloneAllBranches(true).setDirectory(getGitFolder()).setURI(gitUrl)
                    .setCredentialsProvider(getCredentialsProvider()).call();
            repository = git.getRepository();
            callFetch();
        } catch (Exception e) {
            logger.error("Exception on callClone: {}", e);
        } finally {
            if (repository != null) {
                repository.close();
            }
        }

    }
    
    private synchronized void callFetch() throws GitAPIException, IOException {
        long now = System.currentTimeMillis();

        Repository repository = null;
        try {
            repository = getRepository(gitFolder);
            if (git == null) {
                git = new Git(repository);
            }
            int merged = git.fetch().setCredentialsProvider(getCredentialsProvider()).setProgressMonitor(new LoggingProgressMonitor()).call().getTrackingRefUpdates().size();
            Iterable<RevCommit> log = git.log().call();
            lastCommit = log.iterator().next();

            logger.info("Git fetch of branch: {} took: {} millis, resulting in {} fetches.", repository.getBranch(),
                    (System.currentTimeMillis() - now), merged);
        } catch (Exception e) {
        	logger.error("Error in Git pull: ", e);
        } finally {
            if (repository != null) {
                repository.close();
            }
        }
    }
    
    protected UsernamePasswordCredentialsProvider getCredentialsProvider() {
        if (token == null || token.equals("")) {
            return null;
        }
        UsernamePasswordCredentialsProvider upc = new UsernamePasswordCredentialsProvider(token, "");
        return upc;
    }

	@Override
	public String getUrl() {
		return gitUrl;
	}

	@Override
	public String getRepositoryName() {
		return repositoryName;
	}



	@Override
	public void handleEvent(Event event) {
		if(event.getTopic().equals("github/change")) {
			if(this.gitUrl.equals(event.getProperty("url"))) {
				logger.info("Received push event in bare repo. From: {} to: {} url: {} ref: {}",event.getProperty("before"),event.getProperty("head"),event.getProperty("url"),event.getProperty("ref"));
				try {
					callFetch();
				} catch (Throwable e) {
					logger.error("Error: ", e);
				}
				Map<String,Object> properties = new HashMap<>();
				properties.put("name", this.repositoryName);
				properties.put("head", event.getProperty("head"));
				properties.put("before", event.getProperty("before"));
				properties.put("ref", event.getProperty("ref"));
				properties.put("branch", event.getProperty("branch"));
				Event repoChanged = new Event("repository/change",properties);
				eventAdmin.sendEvent(repoChanged);
			} else {
				logger.info("Not responding to this event: {}",event);
			}
		}
	}

    @Reference(name = "EventAdmin", unbind = "clearEventAdmin",policy=ReferencePolicy.DYNAMIC)
    public void setEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = eventAdmin;
    }

    public void clearEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = null;
    }
	
	@Override
	public String getCurrentCommit() {
		return lastCommit.name();
	}

}
