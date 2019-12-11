package com.dexels.appstore.filter;

import java.io.IOException;
import java.util.HashMap;
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
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.TipiCallbackSession;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.sendmessage",immediate=true,property={"osgi.command.scope=tipi","osgi.command.function=sendmessage", "alias=/sendmessage","name=sendmessage","servlet-name=sendmessage","type=webstart"})
public class SendMessage extends BaseOperation implements AppStoreOperation,Servlet {
	
	private final static Logger logger = LoggerFactory
			.getLogger(SendMessage.class);
	
	private static final long serialVersionUID = 4412190224396292038L;
	private final Map<String,TipiCallbackSession> members = new HashMap<String,TipiCallbackSession>();

	
	

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

	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		
		verifyAuthorization(req, resp);
		String session = req.getParameter("session");
		String message = req.getParameter("text");
		System.err.println("Session:"+session);
		System.err.println("Message:"+message);
		//------
		verifyAuthorization(req,resp);
		resp.setContentType("application/json");
		TipiCallbackSession s = members.get(session);
		if(s!=null) {
			s.sendMessage(message);
		} else {
			logger.info("no session found");
			resp.sendError(401,"No session found");
		}
		resp.getWriter().write("ok");
	}

	@Reference(unbind="removeSocket",policy=ReferencePolicy.DYNAMIC,cardinality=ReferenceCardinality.MULTIPLE)
	public void addSocket(TipiCallbackSession s,Map<String,Object> settings) {
		String session = (String) settings.get("session");
		members.put(session,s);
	}

	public void removeSocket(TipiCallbackSession s,Map<String,Object> settings) {
		String session = (String) settings.get("session");
		members.remove(session);
	}

	
	@Override
	public void build(RepositoryInstance a) throws IOException {

	}
}
