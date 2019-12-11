package com.dexels.server.mgmt.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.server.mgmt.shutdown.ServerShutdownImpl;

@Component(name = "dexels.server.mgmt.servlet.shutdown", configurationPolicy = ConfigurationPolicy.REQUIRE, service = {
        Servlet.class }, enabled = true, property = { "alias=/shutdown", "servlet-name=shutdown" })
public class ShutdownServlet extends HttpServlet {

    /**
     * 
     */
    private static final long serialVersionUID = -7600640394232458952L;
    private final static Logger logger = LoggerFactory.getLogger(ShutdownServlet.class);
    private String secret = null;
    private ServerShutdownImpl shutter = null;
	private EventAdmin eventAdmin;

    @Activate
    public void activate(BundleContext bc, Map<String, Object> settings) {
       
        if (System.getenv("SHUTDOWN_KEY") != null) {
            this.secret = System.getenv("SHUTDOWN_KEY");
        } else if (settings.containsKey("secret")) {
            this.secret = (String) settings.get("secret");
        } else {
            logger.warn("Missing shutdown secret! Unable to activate");
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String res = null;
        if (secret == null) {
            logger.warn("Shutdown servlet not configured");
            resp.sendError(500, "Configuration error");
        }
        if (shutter == null) {
            // This shouldn't happen
            logger.warn("Unable to schedule shutdown due to missing ServerShutdownImpl");
            resp.sendError(500, "Configuration error");
        }
        try {
            String ip = req.getHeader("X-Forwarded-For");
            if (ip == null || ip.equals("")) {
                ip = req.getRemoteAddr();
            }

            logger.info("Received Shutdown request from {}", ip);

            String check = req.getParameter("check");
            String timeout = req.getParameter("timeout");
            String cancel = req.getParameter("cancel");

            res = "failed";
            if (secret.equals(check)) {
                if (cancel != null && cancel.equals("true")) {
                    shutter.cancelShutdownInProgress();
                	eventAdmin.postEvent(new Event("navajo/cancelshutdown",Collections.emptyMap()));
                    
                } else {
                    if (timeout != null && !timeout.equals("")) {
                        shutter.setTimeout(Integer.valueOf(timeout));
                    }
                    shutter.start();
                }
                if(eventAdmin!=null) {
                	eventAdmin.postEvent(new Event("navajo/shutdown",Collections.emptyMap()));
                }
                res = "ok";
            } else {
                logger.warn("Received unauthorized Shutdown request!");
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            logger.warn("Exception on scheduling shutdown: ", e);
        }

        resp.setContentType("text/plain");
        PrintWriter writer = resp.getWriter();
        writer.write(res);
        writer.close();
    }
    
    @Reference(unbind="clearShutdownInstance", policy=ReferencePolicy.DYNAMIC)
    public void setShutdownInstance(ServerShutdownImpl i) {
        shutter = i;
    }

    public void clearShutdownInstance(ServerShutdownImpl i) {
        shutter = null;
    }

	@Reference(cardinality=ReferenceCardinality.OPTIONAL,policy=ReferencePolicy.DYNAMIC,unbind="clearEventAdmin")
	public void setEventAdmin(EventAdmin eventAdmin) {
		this.eventAdmin = eventAdmin;
	}
	
	

	public void clearEventAdmin(EventAdmin eventAdmin) {
		this.eventAdmin = null;
	}

}
