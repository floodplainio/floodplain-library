package com.dexels.appstore.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.eclipse.jetty.servlets.GzipFilter;
import org.osgi.service.component.annotations.Component;

@Component(name="tipi.dev.filter", property={"filter-name=compression","urlPatterns=/*","httpContext.id=appstore"}, service={Filter.class},immediate=true)
public class CompressionFilter extends GzipFilter {

	
	@Override
	public void doFilter(ServletRequest req, ServletResponse resp,
			FilterChain chain) throws IOException, ServletException {
		super.doFilter(req, resp, chain);
	}

}
