package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.IOException;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.felix.service.command.Descriptor;
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

@Component(name="tipi.dev.operation.checkout",immediate=true,property={"osgi.command.scope=tipi","osgi.command.function=checkout", "alias=/checkout","name=checkout","servlet-name=checkout","repo=git"})
public class Checkout extends BaseOperation implements AppStoreOperation,Servlet {

	
	private static final long serialVersionUID = 8640712571228602628L;
	private final static Logger logger = LoggerFactory
			.getLogger(Checkout.class);
	
//	public void call(CommandSession session, @Descriptor(value = "The script to call") String scr) {

	
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
	
	@Descriptor(value = "Checkout a specific id") 
	public void checkout(String name,String objectid, String branchname) throws IOException {
		logger.info("Checkout application: {}",name);
		RepositoryInstance as = applications.get(name);
		if(!(as instanceof GitRepositoryInstance)) {
			throw new IOException("Can only pull from a Git application");
		}
		GitRepositoryInstance ha = (GitRepositoryInstance) as;
		ha.callCheckout(objectid,branchname);
	}

	
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		verifyAuthorization(req, resp);
		String app = req.getParameter("app");
		String commitId = req.getParameter("commitId");
		String branchname = req.getParameter("branchName");
		if(app!=null) {
			checkout(app,commitId,branchname);
		}
		writeValueToJsonArray(resp.getOutputStream(),"checkout ok");
	}

	@Override
	public void build(RepositoryInstance a) throws IOException {
	}
}
