package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.osgi.service.component.annotations.Component;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.logout",immediate=true,property={"osgi.command.scope=tipi","osgi.command.function=logout", "alias=/logout","name=logout","servlet-name=logout","type=global"})
public class Logout extends BaseOperation implements AppStoreOperation,Servlet {

	
	private static final long serialVersionUID = 8640712571228602628L;
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		HttpSession session = req.getSession();
		session.invalidate();
		resp.sendRedirect("ui/index.html");
	}
	

	@Override
	public void build(RepositoryInstance a) throws IOException {

	}
}
