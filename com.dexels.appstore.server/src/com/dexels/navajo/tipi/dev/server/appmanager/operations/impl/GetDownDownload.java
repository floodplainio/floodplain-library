package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Enumeration;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetDownDownload extends HttpServlet {

	private static final Logger logger = LoggerFactory.getLogger(GetDownDownload.class);
	private static final long serialVersionUID = -1975818963198435776L;
	private File root;

	public GetDownDownload(File root) {
		this.root = root;
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		Enumeration<String> en = config.getInitParameterNames();
		while (en.hasMoreElements()) {
			String name = en.nextElement();
			logger.info("init: {} -> {}", name, config.getInitParameter(name));
		}
		super.init(config);
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
		try {
			if (request.getPathInfo() == null) {
				response.sendError(400);
				return;
			}
			logger.info("Get: {} request: {}", request.getPathInfo(), request.getPathTranslated());
			String path = request.getPathInfo();
			if (path == null) {
				response.sendError(400);
				return;
			}
			File resolvedFile = new File(root, path);
			Path p = resolvedFile.toPath();
			if (!p.startsWith(root.toPath())) {
				response.sendError(404, "Path error: " + path);
				return;
			}

			if (!resolvedFile.exists()) {
				response.sendError(404, "Can't find: " + path);
				return;
			}
			try (FileInputStream input = new FileInputStream(resolvedFile);
					ServletOutputStream outputStream = response.getOutputStream()) {
				IOUtils.copy(input, outputStream);
			} catch (IOException e) {
				logger.error("Error: ", e);
				response.sendError(500, "Error getting: " + path);

			}
		} catch (IOException e) {
			logger.error("IOException in download servlet: ", e);
		}
	}

}
