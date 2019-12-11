package com.dexels.repository.github.impl;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.repository.github.GitHubRepositoryInstance;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component(name="dexels.repository.github.servlet",immediate=true, property = {"alias=/github", "servlet-name=github"},configurationPolicy=ConfigurationPolicy.REQUIRE)
public class GitHubServlet extends HttpServlet implements Servlet {

	private final static Logger logger = LoggerFactory
			.getLogger(GitHubServlet.class);

	private static final long serialVersionUID = -4415777130543523033L;

	private final Map<String, RepositoryInstance> repositories = new HashMap<String, RepositoryInstance>();
	private final Map<RepositoryInstance, Map<String, Object>> repositorySettings = new HashMap<RepositoryInstance, Map<String, Object>>();


    protected EventAdmin eventAdmin = null;
    
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		String pathInfo = req.getPathInfo();
		if (pathInfo == null) {
			resp.sendError(400, "Bad request path");
			return;
		}
		if (pathInfo.startsWith("/")) {
			pathInfo = pathInfo.substring(1);
		}
		String[] parts = pathInfo.split("/");
		if (parts.length < 2) {
			logger.info("Bad path: " + pathInfo);
			resp.sendError(400, "Bad request path");
			return;
		}
		String name = parts[0];
		String branch = parts[1];
		GitHubRepositoryInstance r = findApplicationByName(name, branch);
		if(r==null) {
			resp.sendError(400, "No repo");
			return;
		}
		r.refreshApplication();
		resp.getWriter().write("Repository refreshed.");
	}

    private Map<String, String> getHeadersInfo(HttpServletRequest request) {
        Map<String, String> map = new HashMap<String, String>();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            map.put(key, value);
        }
        return map;
    }

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		if (!checkGitHubIpRange(req)) {
			resp.sendError(401);
		}

        logger.info("Request for github with headers " + getHeadersInfo(req));

		ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory(); // since 2.1 use mapper.getFactory() instead
		boolean isJson = "application/json".equals( req.getContentType());
		JsonParser parse = null;
		if(isJson) {
			parse = factory.createParser(req.getInputStream());
		} else {
			String p = req.getParameter("payload");
			String decoded = null;
			
			try {
				decoded = URLDecoder.decode(p, "UTF-8");
			} catch (Throwable e) {
				logger.error("Decoding problem... continuing. ", e);
				
			}
			if(decoded == null) {
				// then don't decode
				decoded = p;
			}
			logger.info("Received: \n" + p);
			parse = factory.createParser(decoded);
		}
		// ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// copyResource(baos, p.getInputStream());
		// final byte[] byteArray = baos.toByteArray();
		// application/x-www-form-urlencoded
		JsonNode node = mapper.readTree(parse);
		String pingHeader = req.getHeader("X-GitHub-Event");
		if("push".equals(pingHeader)) {
			processPush(mapper, node);
		}
		resp.getWriter().write("ok");
	}

	@Reference(cardinality=ReferenceCardinality.MULTIPLE, policy=ReferencePolicy.DYNAMIC, name="RepositoryInstance", unbind="removeRepositoryInstance", target="(repo=git)")
	public void addRepositoryInstance(RepositoryInstance a,
			Map<String, Object> settings) {
		if(a==null) {
			logger.warn("Null repositoryinstance in GitHub");
			return;
		}
		String repositoryName = a.getRepositoryName()!=null?a.getRepositoryName():"default";
		repositories.put(repositoryName, a);
		repositorySettings.put(a, settings);
	}

	public void removeRepositoryInstance(RepositoryInstance a) {
		repositories.remove(a.getRepositoryName());
		repositorySettings.remove(a);
	}

	

	private void processPush(ObjectMapper mapper, JsonNode node)
			throws IOException, JsonGenerationException, JsonMappingException {
        try {
            final String url = node.get("repository").get("url").asText();
            String ref = node.get("ref").asText();
            final String branch = ref.substring(ref.lastIndexOf("/") + 1, ref.length());
            // mapper.writerWithDefaultPrettyPrinter().writeValue(System.err, node);
            String head = node.get("after").asText();
            String before = node.get("before").asText();
            sendEvent(before, head, ref, branch, url);
        } catch (Exception e) {
            logger.warn("Error while trying to process push node", node);
            throw e;
        }
		
	}
	
	private void sendEvent(String before, String head, String ref, String branch, String url) {
		Map<String, Object> properties = new HashMap<>();
		properties.put("before", before);
		properties.put("head", head);
		properties.put("ref", ref);
		properties.put("branch", branch);
		properties.put("url", url);
		Event event = new Event("github/change", properties);
		eventAdmin.sendEvent(event);
	}

    @Reference(name = "EventAdmin", unbind = "clearEventAdmin",policy=ReferencePolicy.DYNAMIC)
    public void setEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = eventAdmin;
    }

    public void clearEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = null;
    }

    

	private GitHubRepositoryInstance findApplicationByName(String name,
			String branch) {
		for (Map.Entry<RepositoryInstance, Map<String, Object>> e : repositorySettings
				.entrySet()) {
			final RepositoryInstance instance = e.getKey();
			if (!(instance instanceof GitHubRepositoryInstance)) {
				continue;
			}
			GitHubRepositoryInstance gi = (GitHubRepositoryInstance) instance;
			Map<String, Object> s = e.getValue();
			if (s != null) {
				if (name.equals(s.get("name"))) {
					if (branch.equals(s.get("branch"))) {
						logger.info("Found application for repo with name: "
								+ name + " and branch: " + branch);
						return gi;
					}
				}
			}
		}
		logger.info("Did not find application for repo from url : " + name
				+ " branch: " + branch);
		return null;
	}

	private boolean checkGitHubIpRange(HttpServletRequest req) {
		// The Public IP addresses for these hooks are: 204.232.175.64/27,
		// 192.30.252.0/22.
		return true;
	}
}
