package com.dexels.repository.git.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServletRequest;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.http.server.GitServlet;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.repository.git.GitRepository;
import com.dexels.repository.git.GitRepositoryServer;

@Component(name="dexels.repository.gitserver",immediate=true, configurationPolicy=ConfigurationPolicy.REQUIRE)
public class GitRepositoryServerImpl implements GitRepositoryServer {
	
	private final Map<String,GitRepository> repositories = new HashMap<>();
	private final Map<GitRepository,Map< String,Object>> repositorySettings = new HashMap<>();
//	private final Map<GitRepositoryInstance,ServiceRegistration<Servlet>> servlets = new HashMap<>();
	private final Map<String,FileRepository> gitRepositories = new HashMap<>();
    private AtomicBoolean active = new AtomicBoolean(false);
	private BundleContext bundleContext;
	private ServiceRegistration<Servlet> servletRegistration;

	private final static Logger logger = LoggerFactory.getLogger(GitRepositoryServerImpl.class);

	@Activate
	public void activate(BundleContext context) {
		try {
			this.bundleContext = context;
			this.active.set(true);
			this.servletRegistration = activateServlet();
		} catch (Throwable e) {
			logger.error("Error: ", e);
		}
	}
	
	@Deactivate
	public void deactivate() {
		this.active.set(false);
		if(this.servletRegistration!=null) {
			this.servletRegistration.unregister();
		}
	}
	
	
	@Reference(unbind="removeRepository",policy=ReferencePolicy.DYNAMIC,cardinality=ReferenceCardinality.MULTIPLE)
	public void addRepository(GitRepository repo, Map< String,Object> settings) {
		if(repo==null) {
			logger.warn("Null object added to repo");
			return;
		}
		String repositoryName = repo.getRepositoryName();
		this.repositories.put(repositoryName, repo);
		this.repositorySettings.put(repo, settings);
		try {
			FileRepository jgitRepository = new FileRepository(repo.getGitFolder());
			gitRepositories.put(repositoryName, jgitRepository);
		} catch (IOException e) {
			logger.error("Error: ", e);
		}

	}

	public void removeRepository(GitRepository repo) {
		this.repositories.remove(repo.getRepositoryName());
		this.repositorySettings.remove(repo);
	}

	private ServiceRegistration<Servlet> activateServlet() {
		Servlet gs = getServlet();
		Dictionary<String,Object> properties = new Hashtable<>();
		properties.put("alias", "/git/*");
		properties.put("servlet-name", "gitservlet");
		logger.info("Registering git repository at: "+"/git");
		ServiceRegistration<Servlet> registration = bundleContext.registerService(Servlet.class, gs, properties);
		return registration;
	}


	
	private Servlet getServlet() {
		try {
			GitServlet servlet = new GitServlet();
			servlet.setRepositoryResolver(new RepositoryResolver<HttpServletRequest>() {
				public Repository open(HttpServletRequest req, String name)
						throws RepositoryNotFoundException,
						ServiceNotEnabledException {
					String strippedName = name;
					if(name.endsWith(".git")) {
						strippedName = name.substring(0,name.length()-4);
					}
					FileRepository db = gitRepositories.get(strippedName);
					if(db==null) {
						throw new RepositoryNotFoundException(name);
					} else {
						db.incrementOpen();
						// TODO Beware of leaks
						return db;

					}
				}
			});
			return servlet;
		} catch (Throwable e) {
			logger.error("Error: ", e);
			return null;
		}
	}

	@Override
	public URL getURLforRepo(String repo) {
		String base = System.getenv("GIT_REPOSITORY_BASEURL");
		try {
			if(base!=null) {
				return new URL(base+repo);
			}
			return new URL("https://source.dexels.com/git/"+repo);
		} catch (MalformedURLException e) {
			logger.error("Error: ", e);
		}
		return null;
	}

}
