package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.tipi.dev.core.digest.DigestBuilder;
import com.dexels.navajo.tipi.dev.core.digest.JarBuilder;
import com.dexels.navajo.tipi.dev.core.projectbuilder.BaseDeploymentBuilder;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.impl.RepositoryInstanceWrapper;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.getdown", immediate=true,service={Servlet.class,AppStoreOperation.class,GetDownBuild.class,EventHandler.class},property={"osgi.command.scope=tipi","osgi.command.function=getdown", "alias=/getdown","name=getdown","servlet-name=getdown","type=webstart","event.topics=repository/change"})
public class GetDownBuild extends BaseOperation implements AppStoreOperation,Servlet,EventHandler {
	
	private static final long serialVersionUID = -325075211700621696L;
	private static final Logger logger = LoggerFactory.getLogger(GetDownBuild.class);
	private DigestBuilder digestBuilder;
	private JarBuilder jarBuilder;
	private BundleContext bundleContext;
	private Map<String,ServiceRegistration<Servlet>> downloadServletRegistrations = new HashMap<>();

	@Activate
	public void activate(BundleContext context) {
		this.bundleContext = context;
		discover();
	}
	
	@Deactivate
	@Override
	public void deactivate() {
		logger.info("Deregistering # of servlets: {}", downloadServletRegistrations.size());
		downloadServletRegistrations.values().stream().forEach(ServiceRegistration::unregister);
		downloadServletRegistrations.clear();
	}


	@Reference(unbind="clearDigestBuilder",policy=ReferencePolicy.DYNAMIC)
	public void setDigestBuilder(DigestBuilder builder) {
		this.digestBuilder = builder;
	}
	
	public void clearDigestBuilder(DigestBuilder builder) {
		this.digestBuilder = null;
	}

	
	@Reference(unbind="clearJarBuilder",policy=ReferencePolicy.DYNAMIC)
	public void setJarBuilder(JarBuilder builder) {
		this.jarBuilder = builder;
	}
	
	public void clearJarBuilder(JarBuilder builder) {
		this.jarBuilder = null;
	}
	
	@Override
	@Reference(unbind="clearAppStoreManager",policy=ReferencePolicy.DYNAMIC)
	public void setAppStoreManager(AppStoreManager am) {
		super.setAppStoreManager(am);
	}

	@Override
	public void clearAppStoreManager(AppStoreManager am) {
		super.clearAppStoreManager(am);
	}
	
	@Override
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeRepositoryInstance",policy=ReferencePolicy.DYNAMIC)
	public void addRepositoryInstance(RepositoryInstance a) {
		super.addRepositoryInstance(a);
		if(bundleContext!=null) {
			discover();
		}
	}

	@Override
	public void removeRepositoryInstance(RepositoryInstance a) {
		super.removeRepositoryInstance(a);
	}
	
	@Override
	@Reference(unbind="clearRepositoryManager",policy=ReferencePolicy.DYNAMIC)
	public void setRepositoryManager(RepositoryManager repositoryManager) {
		super.setRepositoryManager(repositoryManager);
	}

	@Override
	public void clearRepositoryManager(RepositoryManager repositoryManager) {
		super.clearRepositoryManager(repositoryManager);
	}	
	
	public void build(String name) throws IOException {
		RepositoryInstance as = applications.get(name);
		if(as==null) {
			for (RepositoryInstance a: applications.values()) {
				buildInstance(a);
			}
			
		} else {
			build(as);
		}
	}


	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		try {
			verifyAuthorization(req, resp);
		} catch (IOException e) {
			logger.error("Authorization error: ", e);
			resp.sendError(401, "Not authorized for operation");
			return;
		}
		String val = req.getParameter("app");
		try {
			if(val!=null) {
				build(val);
			} else {
				build();
			}
			writeValueToJsonArray(resp.getOutputStream(),"build ok");
		} catch (IOException e) {
			logger.error("Build error ", e);
			resp.sendError(401);
		}
		
	}

	
	public void build() {
		for (RepositoryInstance a: applications.values()) {
			try {
				build(a);
			} catch (IOException e) {
				logger.error("Error building: ", e);
			}
		}
	}
	public void discover() {
		this.applications
			.values()
			.stream()
			.forEach(repository->{
		        RepositoryInstanceWrapper a = new RepositoryInstanceWrapper(repository);
		        try {
					a.load();
				} catch (IOException e) {
					logger.error("Load repo error: ", e);
				}
		        a.getProfiles().forEach(profile->{
		        	File profileFolder = new File(repository.getOutputFolder(),profile);
		            File getDownLibrary = new File(profileFolder,"getdown");
		        	registerServlet(repository, profile, getDownLibrary);
		        });
			});
	}
	
	public synchronized void buildInstance(RepositoryInstance repoInstance) throws IOException {
        RepositoryInstanceWrapper a = new RepositoryInstanceWrapper(repoInstance);
        a.load();
        String codebase = appStoreManager.getCodeBase();
        List<String> boilerplate = IOUtils.readLines(this.getClass().getResourceAsStream("getdownboilerplate.txt"), StandardCharsets.UTF_8);
        
        a.getProfiles().forEach(profile->{
            List<String> lines = new ArrayList<>();
        	logger.info("Profile: {}",profile);
        	lines.add("appbase = "+codebase+"/"+a.getRepositoryName()+"/"+profile+"/download/");
        	lines.addAll(boilerplate);
        	lines.add("apparg = start.xml");
        	try {
            	 Map<String, String> result = BaseDeploymentBuilder.parseProfileArguments(a.getRepositoryFolder(), profile, a.getDeployment());
            	 List<String> args = result.entrySet().stream().map(e->"jvmarg = -D"+e.getKey()+"="+e.getValue()).collect(Collectors.toList());
            	 lines.addAll(args);
            	 logger.info("Result: {}", result);
            } catch (IOException e) {
				logger.error("Error parsing arguments: ",e);
			}
        	File profileFolder = new File(repoInstance.getOutputFolder(),profile);
            File getDownLibrary = new File(profileFolder,"getdown");
            lines.add("code = tipi.jar");
            lines.add("code = resource.jar");
            List<String> codeLines = extractCodeLines(a, getDownLibrary);
            lines.addAll(codeLines);
        	writeGetDown(getDownLibrary,lines);
        	try {
	        	jarBuilder.buildJar(a.getRepositoryFolder(),getDownLibrary);
				digestBuilder.buildGetDown(getDownLibrary);
			} catch (IOException e) {
				logger.error("Digest build error: ", e);
			}
        });
	}

	private void registerServlet(RepositoryInstance repoInstance, String profile, File getDownLibrary) {
			String id = repoInstance+"/"+profile;
			ServiceRegistration<Servlet> existing = downloadServletRegistrations.get(id);
			if(existing!=null) {
				existing.unregister();
				downloadServletRegistrations.remove(id);
			}
			
			GetDownDownload downloadServlet = new GetDownDownload(getDownLibrary);
			Dictionary<String, Object> settings = new Hashtable<>();
			settings.put("context.init.resourceBase", getDownLibrary.getAbsolutePath());
			settings.put("alias", "/"+repoInstance.getRepositoryName()+"/"+profile+"/download");
			settings.put("profile", profile);
			settings.put("repo", repoInstance.getRepositoryName());
			ServiceRegistration<Servlet> reg = bundleContext.registerService(Servlet.class, downloadServlet, settings);
			downloadServletRegistrations.put(id, reg);
	}

	private void writeGetDown(File base, List<String> codeLines) {
		try(Writer fw = new FileWriter(new File(base,"getdown.txt"))) {
			for (String line : codeLines) {
				fw.write(line+"\n");
			}
			fw.flush();
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
	}

	private List<String> extractCodeLines(RepositoryInstanceWrapper a, File getDownLibrary) {
		List<String> codeLines = new ArrayList<>();
        a.getDependencies().forEach(e->{
        	URL u = e.getUrl();
        	File path = e.getSimplePathForDependency(getDownLibrary);
        	path.getParentFile().mkdirs();
        	
        	try {
				FileUtils.copyURLToFile(u, path);
			} catch (IOException e1) {
				logger.error("Error: ", e1);
			}
			logger.info("URL: {} -> {}",u,path);
			String relativePath = relativePath(getDownLibrary, path);
			logger.info("Relativize: {}", relativePath);
			codeLines.add("code = "+relativePath);
        });
        return codeLines;
	}
	
	@Override
	public synchronized void build(RepositoryInstance repoInstance) throws IOException {
	    buildInstance(repoInstance);
	}

	@SuppressWarnings("unchecked")
	public void handleEvent(Event event) {
	    logger.info("Received event: {}", event.getTopic());
		RepositoryInstance ri = (RepositoryInstance)event.getProperty("repository");
		Object autobuildobject = ri.getSettings().get("autobuild");
		if(autobuildobject instanceof Boolean) {
			Boolean b = (Boolean) autobuildobject;
			if(!b) {
				logger.warn("Skipping build for non-autobuilding repo");
				return;
			}
		}
		if(autobuildobject instanceof String) {
			String b = (String) autobuildobject;
			if(!"true".equals(b)) {
				logger.warn("Skipping build for non-autobuilding repo");
				return;
			}
		}
		List<String> added = (List<String>) event.getProperty("ADD");
		List<String> modified = (List<String>) event.getProperty("MODIFY");
		List<String> copied = (List<String>) event.getProperty("COPY");
		List<String> deleted = (List<String>) event.getProperty("DELETE");
		List<String> all = new ArrayList<>();
		if(added!=null) {
			all.addAll(added);
		}
		if(copied!=null) {
			all.addAll(copied);
			
		}
		if(modified!=null) {
			all.addAll(modified);
		}
		if(deleted!=null) {
			all.addAll(deleted);
		}

		logger.info("Finished handling event");
		

	}


    private String relativePath(File base, File path) {
        return base.toURI().relativize(path.toURI()).getPath();
    }

}
