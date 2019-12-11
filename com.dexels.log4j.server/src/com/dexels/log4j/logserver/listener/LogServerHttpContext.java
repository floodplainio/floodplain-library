package com.dexels.log4j.logserver.listener;

import java.io.IOException;
import java.net.URL;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.http.HttpContext;

@Component(name="dexels.logserver.httpcontext",property="httpContext.id=dexels.logserver")
public class LogServerHttpContext implements HttpContext {
	
	@Override
	public boolean handleSecurity(HttpServletRequest request, HttpServletResponse response) throws IOException {
		return true;
	}

	@Override
	public URL getResource(String name) {
		return null;
	}

	@Override
	public String getMimeType(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
