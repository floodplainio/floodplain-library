package com.dexels.repository.git.impl;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
import org.eclipse.jgit.api.DiffCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.StoredConfig;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.treewalk.AbstractTreeIterator;
import org.eclipse.jgit.treewalk.CanonicalTreeParser;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.repository.core.impl.RepositoryInstanceImpl;
import com.dexels.repository.git.GitRepository;
import com.dexels.repository.git.GitRepositoryInstance;
import com.dexels.repository.github.impl.GithubUtils;
import com.dexels.server.mgmt.api.ServerHealthProvider;
import com.hazelcast.core.ILock;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;



@Component(name = "dexels.repository.git.repository", configurationPolicy = ConfigurationPolicy.REQUIRE, immediate = true, property = { "repo=git" }, service = { GitRepositoryInstance.class, RepositoryInstance.class, GitRepository.class })
public class  GitRepositoryInstanceImpl extends RepositoryInstanceImpl implements GitRepositoryInstance, Runnable {

    private static final int UPDATE_LOCK_SLEEP_TIME = 5000;
    private static final int DEFAULT_SLEEP_TIME = 60000;

    private final static Logger logger = LoggerFactory.getLogger(GitRepositoryInstanceImpl.class);

    protected RepositoryManager repositoryManager;

    protected Git git;
    protected RevCommit lastCommit;

    protected File privateKey;
    protected File publicKey;
    protected String branch;
    protected String gitUrl;
    protected String name;
    private String commit;
    protected String tag;
    protected Boolean requiredForServerStatus = true;
    
    protected EventAdmin eventAdmin = null;
    protected Thread updateThread = null;
    protected HazelcastService hazelcastService = null;

    protected int sleepTime = -1;
    protected boolean running;

	private File outputFolder;

	private File tempFolder;
    private String currentCommitVersion;
	
    private ServerHealthProvider healthProvider;

    public GitRepositoryInstanceImpl() {
        logger.debug("Instance created!");
    }

    @Reference(name = "EventAdmin", unbind = "clearEventAdmin", cardinality=ReferenceCardinality.OPTIONAL)
    public void setEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = eventAdmin;
    }

    public void clearEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = null;
    }

	@Override
	public File getOutputFolder() {
		return this.outputFolder;
	}
	
	// always return the applicationFolder
	@Override
	public File getTempFolder() {
		return this.tempFolder;
	}
	
    
    @Override
    @Reference(name = "ConfigAdmin", unbind = "clearConfigAdmin")
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        super.setConfigAdmin(configAdmin);
    }

    @Reference(name = "RepositoryManager", unbind = "clearRepositoryManager")
    public void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = repositoryManager;
    }

    public void clearRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = null;
    }
    
    @Reference(name = "HazelcastService", unbind = "clearHazelcastService",cardinality=ReferenceCardinality.OPTIONAL, policy=ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService service) {
        this.hazelcastService = service;
    }

    public void clearHazelcastService(HazelcastService service) {
        this.hazelcastService = null;
    }
    

    @Reference(cardinality=ReferenceCardinality.MULTIPLE, unbind = "removeOperation",policy=ReferencePolicy.DYNAMIC)
    @Override
    public void addOperation(AppStoreOperation op, Map<String, Object> settings) {
        super.addOperation(op, settings);
    }
    
    public void removeOperation(AppStoreOperation op, Map<String, Object> settings) {
    	super.removeOperation(op, settings);
    }
    
    @Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearServerHealthProvider", cardinality=ReferenceCardinality.OPTIONAL)
    public void setServerHealthProvider(ServerHealthProvider provider) {
        healthProvider = provider;
    }
    
    public void clearServerHealthProvider(ServerHealthProvider provider) {
        healthProvider = null;
    }

    
    @Activate
    public void activate(Map<String, Object> settings) throws IOException, InvalidRemoteException, TransportException,
    GitAPIException {
    	this.modify(settings);
    }

	@Modified
    public void modify(Map<String, Object> settings) throws IOException, InvalidRemoteException, TransportException,
    GitAPIException {
        File gitRepoFolder = repositoryManager.getRepositoryFolder();
        setSettings(settings);

        gitUrl = (String) settings.get("url");
        String key = (String) settings.get("key");
        branch = (String) settings.get("branch");
        name = (String) settings.get("name");
        commit = (String) settings.get("commit");
        tag = (String) settings.get("tag");
        
        this.type = (String) settings.get("repository.type");
        if (this.type == null) {
            logger.warn("type key in GitRepo configuration is deprecated, use repository.type");
            this.type = (String) settings.get("type");
        }
        
        if (settings.containsKey("requiredForServerStatus")) {
            this.requiredForServerStatus = Boolean.valueOf((String) settings.get("requiredForServerStatus"));
        }
        
        deployment = (String) settings.get("repository.deployment");
        repositoryName = determineRepositoryName(settings);
        applicationFolder = new File(gitRepoFolder, repositoryName);

		outputFolder = new File(repositoryManager.getOutputFolder(),repositoryName);
		tempFolder = new File(repositoryManager.getTempFolder(),repositoryName);

        logger.info("Activating/Modifying git repo. Folder: {} Deployment {} Branch: {} GitUrl: {} Commit: {}",
                applicationFolder.getAbsolutePath(), deployment, branch, gitUrl,commit);
        super.setSettings(settings);

        File keyFolder = repositoryManager.getSshFolder();
        if (keyFolder != null && keyFolder.exists() && key != null) {
            privateKey = null;
            privateKey = new File(keyFolder, key);
            if (!privateKey.exists() || !privateKey.isFile()) {
                privateKey = null;
            }
            publicKey = null;
            publicKey = new File(keyFolder, key + ".pub");
            if (!publicKey.exists() || !publicKey.isFile()) {
                publicKey = null;
            }
        }

        if (applicationFolder.exists()) {
            Repository repository = getRepository(applicationFolder);
            git = new Git(repository);

            repository.close();
            refreshApplication();
            
        } else {
        	logger.info("Folder: "+applicationFolder.getAbsolutePath()+" does not exist. Cloning from source");
            callClone();
        }

        
        registerFileInstallLocations(parseLocations((String) settings.get("fileinstall")));
//        initializeThread(settings);
    }

	private String determineRepositoryName(Map<String, Object> settings) {
        String settingsRepoName = (String) settings.get("repositoryname");
        if(settingsRepoName!=null) {
        	settingsRepoName = settingsRepoName.trim();
        }
        if(settingsRepoName!=null && !"".equals(settingsRepoName)) {
        	return settingsRepoName;
        }

		return name + "-" + branch;
	}
    

    public String getGitUrl() {
        return gitUrl;
    }
    
    

    
    @Override
    public boolean requiredForServerStatus() {
        return requiredForServerStatus;
    }

    @Override
    public String getLastCommitVersion() {
        if (lastCommit != null) {
            return lastCommit.getId().name();
        }
        return null;
    }

    public String getLastCommitMessage() {
        if (lastCommit != null) {
            return lastCommit.getFullMessage();
        }
        return null;
    }

    public String getLastCommitDate() {
        if (lastCommit != null) {
            PersonIdent authorIdent = lastCommit.getAuthorIdent();
            if (authorIdent != null) {
                final Date when = authorIdent.getWhen();

                if (when != null) {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(when);
                }
                return null;
            }
        }
        return null;
    }

    public String getLastCommitAuthor() {
        if (lastCommit != null) {
            PersonIdent authorIdent = lastCommit.getAuthorIdent();
            if (authorIdent != null) {
                return authorIdent.getName();
            }
        }
        return null;
    }

    public String getLastCommitEmail() {
        if (lastCommit != null) {
            PersonIdent authorIdent = lastCommit.getAuthorIdent();
            if (authorIdent != null) {
                return authorIdent.getEmailAddress();
            }
        }
        return null;
    }

    public String getBranch() {
        return branch;
    }

    @Deactivate
    public void deactivate() {
        super.deregisterFileInstallLocations();
        this.running = false;
        if (updateThread != null) {
            updateThread.interrupt();
        }
    }

    

    private List<String> parseLocations(String locations) {
        if (locations == null || "".equals(locations)) {
            return Collections.emptyList();
        }
        return Arrays.asList(locations.split(","));
    }

    private int parseSleepTime(Object value) {
        if (value == null) {
            return -1;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);

        }
        return -1;
    }

    @Override
    public void callClean() throws IOException {
        File gitSubfolder = new File(applicationFolder, ".git");
        if (!gitSubfolder.exists()) {
            logger.info("Folder: " + applicationFolder.getAbsolutePath() + " is not a git repo. Not pulling.");
        }
        Repository repository = null;
        try {
            repository = getRepository(applicationFolder);
            git = new Git(repository);
            git.clean().call();
            logger.info("Git clean complete.");
        } catch (NoWorkTreeException e) {
        	throw new IOException("Clean failed, git problem: ",e);
		} catch (GitAPIException e) {
        	throw new IOException("Clean failed, git problem: ",e);
		} finally {
            if (repository != null) {
                repository.close();
            }
        }
    }

    @Override
    public synchronized void callPull() throws IOException {
        long now = System.currentTimeMillis();

        File gitSubfolder = new File(applicationFolder, ".git");
        if (!gitSubfolder.exists()) {
            logger.info("Folder: " + applicationFolder.getAbsolutePath() + " is not a git repo. Not pulling.");
        }
        Repository repository = null;
        try {
            repository = getRepository(applicationFolder);
            if (git == null) {
                git = new Git(repository);
            }
           
            if (this.commit != null) {
                git.fetch().setCredentialsProvider(getCredentialsProvider()).call();

            	Iterable<RevCommit> log = git.log().call();
                RevCommit currentCommit = log.iterator().next();

                ObjectId commitId = ObjectId.fromString(this.commit);
                try(RevWalk revWalk = new RevWalk( repository )) {
	            	RevCommit commit = revWalk.parseCommit( commitId );
	            	if(!commit.equals(currentCommit)) {
	            		logger.info("Wrong commit id, changing checkout");
//	            		git.checkout().setStartPoint(commit).call();
	            		git.checkout().setName(this.commit).call();
	            		logger.info("Checkout done");
	            	}
                }
            	return;
            }
            
            String currentBranch = repository.getBranch();
            if (!currentBranch.equals(branch)) {
                logger.warn("Wrong branch seems to be checked out. Current: {} Expected: {} Repository: {}",currentBranch,branch,applicationFolder.getName());
            }
            git.checkout().setName(branch).call();
            // TODO check local changes, see if a hard reset is necessary
            git.reset().setMode(ResetType.HARD).call();
            int merged = git.pull().setCredentialsProvider(getCredentialsProvider()).setProgressMonitor(new LoggingProgressMonitor()).call().getFetchResult()
                    .getTrackingRefUpdates().size();

            Iterable<RevCommit> log = git.log().call();
            lastCommit = log.iterator().next();

            logger.info("Git pull of branch: {} took: {} millis, resulting in {} fetches.", repository.getBranch(),
                    (System.currentTimeMillis() - now), merged);
        } catch (Exception e) {
        	logger.error("Error in Git pull: ", e);
        } finally {
            if (repository != null) {
                repository.close();
            }
        }
    }

    @Override
    public void callCheckout(String objectId, String branchname) throws IOException {
        Repository repository = null;
        try {
            repository = getRepository(applicationFolder);
            git = new Git(repository);
            git.checkout().setCreateBranch(true).setName(branchname).setStartPoint(objectId).setForce(true)
                    .setUpstreamMode(SetupUpstreamMode.TRACK).setStartPoint(branchname).call();
            logger.info("Current branch: " + repository.getBranch());
            git.clean().call();
            git.reset().setMode(ResetType.HARD).call();
            Iterable<RevCommit> log = git.log().call();
            lastCommit = log.iterator().next();
            this.branch = branchname;
        } catch (Exception e) {
        	logger.error("Error in Git Checkout: ", e);
        } finally {
            if (repository != null) {
                repository.close();
            }
        }
    }

    @Override
    public void callClone() throws IOException {

        Repository repository = null;
        try {
            repository = getRepository(applicationFolder);
            git = Git.cloneRepository().setProgressMonitor(new LoggingProgressMonitor()).setBare(false)
                    .setCloneAllBranches(true).setDirectory(applicationFolder).setURI(getUrl())
                    .setCredentialsProvider(getCredentialsProvider()).call();
            repository = git.getRepository();
            
            if(commit!=null) {
            	git.checkout().setName(commit).call();
            } else if(tag!=null) {
                git.checkout().setName(tag).call();
            } else {
				git.branchCreate().setName(branch).setUpstreamMode(SetupUpstreamMode.SET_UPSTREAM)
						.setStartPoint("origin/" + branch).setForce(true).call();
				git.checkout().setName(branch).call();
				StoredConfig config = git.getRepository().getConfig();
				config.setString("remote", "origin", "fetch", "+refs/*:refs/*");
				config.setString("remote", "origin", "fetch", "+refs/heads/*:refs/remotes/origin/*");
				config.setString("branch", branch, "merge", "refs/heads/" + branch);
				config.setString("branch", branch, "remote", "origin");
				config.save();
				callPull();
				currentCommitVersion = getLastCommitVersion();
            }
        } catch (Exception e) {
            logger.error("Exception on callClone: {}", e);
        
        } finally {
            if (repository != null) {
                repository.close();
            }
        }

    }

    protected UsernamePasswordCredentialsProvider getCredentialsProvider() {
        JSch jsch = new JSch();
        UsernamePasswordCredentialsProvider upc = null;

        JSch.setLogger(new com.jcraft.jsch.Logger() {
            @Override
            public void log(int level, String txt) {
                logger.debug(txt);
            }

            @Override
            public boolean isEnabled(int arg0) {
                return true;
            }
        });
        try {
        	if(privateKey!=null && publicKey!=null) {
                jsch.addIdentity(privateKey.getAbsolutePath(), publicKey.getAbsolutePath());
        	}
        } catch (JSchException e) {
            logger.error("Error: ", e);
        }
        upc = new UsernamePasswordCredentialsProvider("******", "********");
        return upc;
    }

    private Repository getRepository(File basePath) throws IOException {
        FileRepositoryBuilder builder = new FileRepositoryBuilder();
        Repository repository = builder.setGitDir(new File(basePath, ".git")).readEnvironment().findGitDir().build();
        
        return repository;
    }

    public static void main(String[] args) throws IOException, GitAPIException {
        // FileRepositoryBuilder builder = new FileRepositoryBuilder();
        Git git = Git.open(new File("/Users/frank/git/com.sportlink.serv"));
        List<Ref> aa = git.branchList().call();
        System.err.println("aa: " + aa + " size: " + aa.size());
        // git.branchCreate().setName("Acceptance").call();
        Iterable<RevCommit> log = git.log().call();
        RevCommit lastCommit = log.iterator().next();
        System.err.println("LastCommit: "+lastCommit.name());
        // git.reset().setMode(ResetType.HARD).call();
//        PullResult pr = git.pull().call();
//        System.err.println("From: " + pr.getFetchedFrom());
//        System.err.println("Success: " + pr.isSuccessful());
//        System.err.println("C/O conf: " + pr.getMergeResult().getCheckoutConflicts());
//        System.err.println("Fetch: " + pr.getFetchResult().getMessages());
//        System.err.println("failed: " + pr.getMergeResult().getFailingPaths());
//        ObjectId[] merged = pr.getMergeResult().getMergedCommits();
//        if (merged == null) {
//            System.err.println("Merged seems null.");
//        } else {
//            System.err.println("# of merged refs: " + merged.length);
//            for (ObjectId objectId : merged) {
//                System.err.println("ObjectId: " + objectId.toString());
//            }
//        }
//        ObjectId base = pr.getMergeResult().getBase();
//        System.err.println("base: " + base.toString());
//        ObjectId head = pr.getMergeResult().getNewHead();
//        System.err.println("base: " + head.toString());

        // git.checkout().setName("Production").setForce(true).call();
        // git.checkout().setStartPoint("25f17284bc94236a9f921e08aebf36b3c143f2e0").setName("Production").setCreateBranch(true).setForce(true).call();
    }

    public List<DiffEntry> diff(String oldHash) throws IOException, GitAPIException {
        Repository repository = getRepository(applicationFolder);
        try( Git git = new Git(repository)){
            DiffCommand diff = git.diff().setShowNameAndStatusOnly(true).setOldTree(getTreeIterator(repository, oldHash));
            diff.setNewTree(getTreeIterator(repository, "HEAD"));
            List<DiffEntry> entries = diff.call();
            return entries;
        }

    }

    private AbstractTreeIterator getTreeIterator(Repository repository, String name) throws IOException {
        final ObjectId id = repository.resolve(name);
        if (id == null)
            throw new IllegalArgumentException(name);
        final CanonicalTreeParser p = new CanonicalTreeParser();
        final ObjectReader or = repository.newObjectReader();
        try (RevWalk revWalk = new RevWalk(repository)){
			p.reset(or, revWalk.parseTree(id));
            return p;
        } finally {
            or.close();
        }
    }

    @Override
    public String getUrl() {
        return gitUrl;
    }
    
    
    public synchronized void checkRefreshApplication() throws IOException {
        long now = System.currentTimeMillis();
        refreshApplication();
        long elapsed = System.currentTimeMillis() - now;
        logger.debug("Refresh app iteration took: " + elapsed + " millis.");
    }

    
    @Override
    public synchronized void refreshApplication() throws IOException {
        // RevCommit last = lastCommit;
        
        currentCommitVersion = getLastCommitVersion();
        logger.debug(">>> last commit version: " + currentCommitVersion);
        try {
            callPull();

            String newVersion = getLastCommitVersion();
            GithubUtils.checkForCommitChanges(this,eventAdmin,newVersion,currentCommitVersion);
            currentCommitVersion = newVersion;
        } catch (GitAPIException e) {
            logger.error("Error: ", e);
            throw new IOException("Trouble updating git repository.", e);
        }

    }
    
    /** Checks whether my commit hash matches the one we are actually at. If not, 
     * the repository was updated outside of us and we should trigger FileChanged 
     * events.
     */
    protected void checkCommitHash() {
        try {
            // Update last commit
            Iterable<RevCommit> log = git.log().call();
            lastCommit = log.iterator().next();
            if (!lastCommit.getId().name().equals(currentCommitVersion)) {
                logger.info("Diff commit detected old: {} new: {}  - going to detect changes and trigger change events",
                        currentCommitVersion, lastCommit.getId().name());
                String newVersion = getLastCommitVersion();
                GithubUtils.checkForCommitChanges(this,eventAdmin,newVersion,currentCommitVersion);
                currentCommitVersion = newVersion;
            }
           
            
            
        } catch (GitAPIException | IOException e) {
            logger.error("Unable to get last commit ");
            return;
        }
        
        
        
    }

    @Override
    public Map<String, Object> getSettings() {
        Map<String, Object> result = super.getSettings();
        result.put("gitUrl", getGitUrl());
        result.put("lastCommitAuthor", getLastCommitAuthor());
        result.put("lastCommitDate", getLastCommitDate());
        result.put("lastCommitEmail", getLastCommitEmail());
        result.put("lastCommitMessage", getLastCommitMessage());
        result.put("lastCommitVersion", getLastCommitVersion());
        return result;
    }

    @Override
    public String repositoryType() {
        return "git";
    }

    @Override
    public String applicationType() {
        return type;
    }

    protected void initializeThread(Map<String, Object> settings) {
        this.sleepTime = parseSleepTime(settings.get("sleepTime"));

        if (sleepTime < 0) {
            logger.info("No sleepTime specified, using default.");
            sleepTime = DEFAULT_SLEEP_TIME;
        }
        updateThread = new Thread(this);
        this.running = true;
        updateThread.start();
        logger.info("Auto pull sync started!");
    }

    @Override
    public void run() {
        while (running) {
            try {
                checkRefreshApplication();
                checkCommitHash(); 
            } catch (Throwable e) {
                logger.error("Error: ", e);
            }
            
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                running = false;
            }
        }
    }

    @Override
    public void refreshApplicationLocking() throws IOException {
        if (hazelcastService == null) {
            // No locking possible 
            logger.info("Hazelcast service is null, updating without locking...");
            refreshApplication();
            return;
        }
        
        if (hazelcastService.getDeclaredClusterSize() < 2) {
            // No locking needed 
            logger.info("Declared cluster size < 2, updating without locking...");
            refreshApplication();
            return;
        }
        
        while (hazelcastService.getDeclaredClusterSize() > 1 && hazelcastService.getClusterSize() < 2) {
            logger.warn("Defined cluster size is > 1, but we have less than 2 members up!"
                    + " Refusing to update to prevent bringing last surviving cluster member down");
           
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
            }
        }
        
        
        
        ILock lock = hazelcastService.getLock("GitRefresh");
        lock.lock();
        try {
            logger.info("Got my update lock, going to update...");
            refreshApplication();
            /* 
             *  Any changes in the commit will be performed using events. We are going to
             *  sleep some time to allow these events to propagate and be handled. After
             *  that, we perform a basic sanity check to determine whether we seem to be
             *  OK and can release the upgrade lock - letting someone else upgrade.
            */
            
            Thread.sleep(UPDATE_LOCK_SLEEP_TIME);
            while (!postUpdateSystemCheckOK()) {
                logger.info("Post update check indicates we are not up and running yet. Going to wait a bit longer while holding on to my lock...");
                Thread.sleep(UPDATE_LOCK_SLEEP_TIME);
            }
            
        } catch (InterruptedException e) {
            // No more sleeping?
            
        } finally {
            lock.unlock();
        }
        logger.info("Update performed");
    }

	@Override
	public File getGitFolder() {
		File app = getRepositoryFolder();
		File gitFolder = new File(app,".git");
		if(gitFolder.exists()) {
			return gitFolder;
		}
		logger.info("No gitfolder found? Assuming bare repo");
		return app;
	}

	@Override
	public String getCurrentCommit() {
		return lastCommit.toString();
	}

	@Override
	public Map<String, Object> fileChangesBetweenVersions(final String currentCommitVersion, final String newVersion) throws IOException {
		return GithubUtils.fileChangesBetweenVersions(this, currentCommitVersion, newVersion);
	}
	
	  
	@Override
    protected boolean postUpdateSystemCheckOK() {
        if (healthProvider == null) {
            // If we have a currentCommitVersion, then healthProvider should not be null! Otherwise we 
            // are just starting up
            return currentCommitVersion == null;
        }
        return healthProvider.isOk();
    }


}
