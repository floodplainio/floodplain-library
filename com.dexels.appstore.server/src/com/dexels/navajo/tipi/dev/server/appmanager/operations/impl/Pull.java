package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.IOException;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;
import com.dexels.repository.git.GitRepositoryInstance;
//@Component(name="tipi.dev.operation.pull",immediate=true,property={"osgi.command.scope=tipi","httpContext.id=appstore","osgi.command.function=pull", "alias=/pull","name=pull","servlet-name=pull","repo=git"})

@Component(name="tipi.dev.operation.pull",immediate=true,property={"alias=/pull","repo=git","servlet-name=pull"})
public class Pull extends BaseOperation implements Servlet {

	private static final long serialVersionUID = 8640712571228602628L;
	private final static Logger logger = LoggerFactory.getLogger(Pull.class);
	
	@Activate
	public void activate(Map<String,Object> settings) {
		super.activate(settings);
	}
	
	@Deactivate
	public void deactivate() {
		super.deactivate();
	}
	
	@Reference(unbind="clearAppStoreManager",policy=ReferencePolicy.DYNAMIC)
	public void setAppStoreManager(AppStoreManager am) {
		super.setAppStoreManager(am);
	}

	public void clearAppStoreManager(AppStoreManager am) {
		super.clearAppStoreManager(am);
	}
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeRepositoryInstance",policy=ReferencePolicy.DYNAMIC)
	public void addRepositoryInstance(RepositoryInstance a) {
		super.addRepositoryInstance(a);
	}

	public void removeRepositoryInstance(RepositoryInstance a) {
		super.removeRepositoryInstance(a);
	}
	
	@Reference(unbind="clearRepositoryManager",policy=ReferencePolicy.DYNAMIC)
	public void setRepositoryManager(RepositoryManager repositoryManager) {
		super.setRepositoryManager(repositoryManager);
	}

	public void clearRepositoryManager(RepositoryManager repositoryManager) {
		super.clearRepositoryManager(repositoryManager);
	}	

	public void pull(String name) throws IOException {
		logger.info("Pull application: {}",name);
		RepositoryInstance as = applications.get(name);
		build(as);
	}
	
	public void pull() throws IOException {
		logger.info("Pull all applications");
		for (RepositoryInstance a: applications.values()) {
			build(a);
		}
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		verifyAuthorization(req, resp);
		String val = req.getParameter("app");
		if(val!=null) {
			pull(val);
		} else {
			pull();
		}
		writeValueToJsonArray(resp.getOutputStream(),"pull ok");
	}

	@Override
	public void build(RepositoryInstance a) throws IOException {
		if(!(a instanceof GitRepositoryInstance)) {
			throw new IOException("Can only pull from a Git application");
		}
		GitRepositoryInstance ha = (GitRepositoryInstance) a;
		ha.refreshApplication();
	}
}
