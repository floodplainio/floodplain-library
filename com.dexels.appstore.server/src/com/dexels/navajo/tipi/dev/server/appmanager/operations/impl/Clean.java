package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;
import com.dexels.repository.git.GitRepositoryInstance;

@Component(name="tipi.dev.operation.clean", immediate=true, service={Servlet.class,AppStoreOperation.class},property={"osgi.command.scope=tipi","osgi.command.function=clean", "alias=/clean","name=clean","servlet-name=clean"})
public class Clean extends BaseOperation implements AppStoreOperation {

	
	private static final long serialVersionUID = 8640712571228602628L;
	private static final Logger logger = LoggerFactory
			.getLogger(Clean.class);
	
	
	
	@Override
	@Activate
	public void activate(Map<String,Object> settings) {
		super.activate(settings);
	}
	
	@Override
	@Deactivate
	public void deactivate() {
		super.deactivate();
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
	
	public void clean(String name) throws IOException {
		logger.info("Cleaning application: {}",name);
		RepositoryInstance as = applications.get(name);
		build(as);
	}
	
	public void clean() throws IOException {
		logger.info("Cleaning all applications");
		for (RepositoryInstance a: applications.values()) {
			build(a);
		}
	}
	
	
	
	@Override
	// Maybe we should protect this one, it is kind of destructive
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		try {
			verifyAuthorization(req, resp);
		} catch (IOException e) {
			resp.sendError(401);
			return;
		}
		try {
			clean();
			writeValueToJsonArray(resp.getOutputStream(),"clean ok");
		} catch (IOException e) {
			resp.sendError(401);
		}
	}
	
	@Override
	public void build(RepositoryInstance a) throws IOException {
		if(a instanceof GitRepositoryInstance) {
			((GitRepositoryInstance)a).callClean();
		}
		File lib = new File(a.getRepositoryFolder(),"lib");
		if(lib.exists()) {
			FileUtils.deleteQuietly(lib);
		}
		File xsd = new File(a.getRepositoryFolder(),"xsd");
		if(xsd.exists()) {
			FileUtils.deleteQuietly(xsd);
		}
		File digest = new File(a.getRepositoryFolder(),"resource/remotedigest.properties");
		if(digest.exists()) {
			FileUtils.deleteQuietly(digest);
		}
		
		File[] jnlps = a.getRepositoryFolder().listFiles((FilenameFilter) (dir, name) -> name.endsWith(".jnlp"));		
		for (File file : jnlps) {
			FileUtils.deleteQuietly(file);
		}
	}

}
