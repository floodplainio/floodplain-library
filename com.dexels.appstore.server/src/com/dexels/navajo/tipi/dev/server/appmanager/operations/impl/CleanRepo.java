package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.File;
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

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.cleanrepo",immediate=true,service={Servlet.class,AppStoreOperation.class},property={"osgi.command.scope=tipi","osgi.command.function=cleanrepo", "alias=/cleanrepo","name=cleanrepo","servlet-name=cleanrepo","type=global"})
public class CleanRepo extends BaseOperation implements AppStoreOperation {

	
	
	private static final long serialVersionUID = -3363914555886806226L;


	@Activate
	@Override
	public void activate(Map<String,Object> settings) {
		super.activate(settings);
	}
	
	@Deactivate
	@Override
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
	
	@Reference(unbind="clearRepositoryManager",policy=ReferencePolicy.DYNAMIC)
	@Override
	public void setRepositoryManager(RepositoryManager repositoryManager) {
		super.setRepositoryManager(repositoryManager);
	}
	
	@Override
	public void clearRepositoryManager(RepositoryManager repositoryManager) {
		super.clearRepositoryManager(repositoryManager);
	}	

	

	public void cleanrepo() {
		File repo = new File(getRepositoryManager().getOutputFolder(), "repo");
		FileUtils.deleteQuietly(repo);
	}
	
	@Override
	public void build(RepositoryInstance a) throws IOException {
		//
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
		cleanrepo();
		try {
			writeValueToJsonArray(resp.getOutputStream(),"cleanrepo  ok");
		} catch (IOException e) {
			resp.sendError(401);
		}
	}
}
