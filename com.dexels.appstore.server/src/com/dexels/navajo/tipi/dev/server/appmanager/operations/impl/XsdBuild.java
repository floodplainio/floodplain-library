package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.File;
import java.io.FilenameFilter;
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

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.tipi.dev.core.projectbuilder.XsdBuilder;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.xsdbuild",immediate=true,property={"osgi.command.scope=tipi","osgi.command.function=xsd", "alias=/xsd","name=xsd","servlet-name=xsd","type=webstart"})
public class XsdBuild extends BaseOperation implements AppStoreOperation, Servlet {

	
	private static final long serialVersionUID = -7219236999229829020L;
	private final static Logger logger = LoggerFactory
			.getLogger(XsdBuild.class);
	


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

	
	public void xsd(String name) throws IOException {
		RepositoryInstance as = applications.get(name);
		build(as);
	}
	
	public void xsd() throws IOException {
		for (RepositoryInstance a: applications.values()) {
			build(a);
		}
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		String val = req.getParameter("app");
		verifyAuthorization(req, resp);
		if(val!=null) {
			xsd(val);
		} else {
			xsd();
		}
		writeValueToJsonArray(resp.getOutputStream(),"xsd build ok");

	}
	
	@Override
	public void build(RepositoryInstance a) throws IOException {
		XsdBuilder xsd = new XsdBuilder();
		File lib = new File(a.getRepositoryFolder(),"lib");
		File[] jars = lib.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".jar");
			}
		});
		if(jars==null || jars.length==0) {
			logger.warn("Can not write xsd: No jar files built.");
			return;
		}
		for (File file : jars) {
			xsd.addJar(file);
		}
		xsd.writeXsd(a.getRepositoryFolder());
	}
}
