package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.felix.service.command.CommandSession;
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
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreData;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.TipiCallbackSession;
import com.dexels.navajo.tipi.dev.server.appmanager.impl.RepositoryInstanceWrapper;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.list",immediate=true,property={"osgi.command.scope=tipi","osgi.command.function=list", "alias=/list","name=list","servlet-name=list","type=global"})
public class List extends BaseOperation implements AppStoreOperation,AppStoreData,Servlet {

	private final Set<AppStoreOperation> operations = new HashSet<AppStoreOperation>();
	private static final long serialVersionUID = 8640712571228602628L;

	
	private final static Logger logger = LoggerFactory.getLogger(List.class);

	
	
	@Activate
	public void activate(Map<String,Object> settings) {
		super.activate(settings);
		logger.info("AppStoreData activated");
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

	
	public void list(CommandSession session ) throws IOException {
		writeValueToJsonArray(session.getConsole(),getApplicationData());
	}

	@Override
	public Map<String,Map<String,?>> getApplicationData() {
		java.util.List<RepositoryInstanceWrapper> ll = new ArrayList<RepositoryInstanceWrapper>(getApplications().values());
		Collections.sort(ll);
		Map<String,Map<String,?>> wrap = new LinkedHashMap<String, Map<String,?>>();
		final Map<String,RepositoryInstanceWrapper> extwrap = new LinkedHashMap<String, RepositoryInstanceWrapper>();
		

		for (RepositoryInstanceWrapper repository : ll) {
			Map<String, TipiCallbackSession> sessionMap = callbackSessions.get(repository.getRepositoryName());
			repository.setSessions(sessionMap);
			extwrap.put(repository.getRepositoryName(), repository);
		}
		wrap.put("applications", extwrap);
		wrap.put("settings", getSettings());


		return wrap;
	}

	protected Map<String,RepositoryInstanceWrapper> getApplications() {
		Map<String,RepositoryInstanceWrapper> result = new HashMap<String, RepositoryInstanceWrapper>();
		for (Map.Entry<String, RepositoryInstance> e : applications.entrySet()) {
			result.put(e.getKey(), new RepositoryInstanceWrapper(e.getValue()));
		}
		return result;
	}
	
	private Map<String, Object> getSettings() {
		Map<String,Object> settings = new HashMap<String, Object>();
		String manifestCodebase = appStoreManager.getManifestCodebase();
		settings.put("manifestcodebase", manifestCodebase);
		settings.put("codebase", appStoreManager.getCodeBase());
		settings.put("organization", appStoreManager.getOrganization());
		settings.put("sessions", appStoreManager.getSessionCount());
		final java.util.List<String> commandNames = new ArrayList<String>();
		for (AppStoreOperation a : operations) {
			commandNames.add(a.getName());
		}
		settings.put("globalCommands", commandNames);
		return settings;
	}

	@Reference(unbind="removeOperation",cardinality=ReferenceCardinality.MULTIPLE,target="(type=global)",policy=ReferencePolicy.DYNAMIC)
	public void addOperation(AppStoreOperation op) {
		operations.add(op);
	}

	public void removeOperation(AppStoreOperation op) {
		operations.remove(op);
		
	}
	
	private final Map<String,Map<String,TipiCallbackSession>> callbackSessions = new HashMap<>();
	@Reference(unbind="removeTipiCallbackSession",cardinality=ReferenceCardinality.MULTIPLE,policy=ReferencePolicy.DYNAMIC)
	public void addTipiCallbackSession(TipiCallbackSession session) {
		Map<String,TipiCallbackSession> applicationsessions = callbackSessions.get(session.getApplication());
		if(applicationsessions==null) {
			applicationsessions = new HashMap<>();
			callbackSessions.put(session.getApplication(), applicationsessions);
		}
		applicationsessions.put(session.getSessionId(), session);
	}

	
	public void removeTipiCallbackSession(TipiCallbackSession session) {
		callbackSessions.get(session.getApplication()).remove(session.getSessionId());
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		verifyAuthorization(req,resp);
		resp.setContentType("application/json");
		
		Map<String,Map<String,?>> wrap = getApplicationData();
		Map<String,String> user = new HashMap<String, String>();
		wrap.put("user", user);
		HttpSession session = req.getSession();
		user.put("login", (String)session.getAttribute("username"));
		user.put("image", (String)session.getAttribute("image"));
		writeValueToJsonArray(resp.getOutputStream(),wrap);
	}

	@Override
	public void build(RepositoryInstance a) throws IOException {

	}
}
