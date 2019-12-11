package com.dexels.log4j.logserver.listener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.InflaterInputStream;

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


@Component(name="dexels.log4j.server.servlet",service=Servlet.class, immediate=true, property={"alias=/logserver","servlet-name=logserver","httpContext.id=dexels.logserver"},configurationPolicy=ConfigurationPolicy.OPTIONAL)
public class LogServerServlet extends HttpServlet {
    public static String LOG_TOPIC = "logserver/logentry";
    private final static Logger logger = LoggerFactory.getLogger(LogServerServlet.class);
    private static final long serialVersionUID = 8297735604970605787L;

    public static final String COMPRESS_GZIP = "gzip";
    public static final String COMPRESS_JZLIB = "jzlib";

    private EventAdmin eventAdmin = null;

    @Reference(cardinality=ReferenceCardinality.MANDATORY,policy=ReferencePolicy.DYNAMIC,unbind="clearEventAdmin")
    public void setEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = eventAdmin;
    }

    public void clearEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = null;
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("received-at", String.valueOf(new Date().getTime()));
        
        BufferedReader r = null;
        Writer writer = new StringWriter();
        
        String incomingEncoding = request.getHeader("Content-Encoding");
        if (incomingEncoding != null) {

            if (incomingEncoding.equals(COMPRESS_JZLIB)) {
                r = new BufferedReader(new java.io.InputStreamReader(new InflaterInputStream(request.getInputStream()), "UTF-8"));
            } else if (incomingEncoding.equals(COMPRESS_GZIP)) {
                r = new BufferedReader(new java.io.InputStreamReader(new java.util.zip.GZIPInputStream(request.getInputStream()), "UTF-8"));
            } else {
                throw new IOException("Unsupported encoding!");
            }
        } else {
            r = new BufferedReader(request.getReader());
        }

        String line = null;
        while ((line = r.readLine()) != null) {
            writer.append(line);
        }
        sendLogEvent(LOG_TOPIC, writer.toString());
    }

    private void sendLogEvent(String topic, String logEvent) {
        if (eventAdmin == null) {
            logger.warn("No event administrator, not sending any events");
            return;
        }
        Map<String, Object> properties = new HashMap<>();
        properties.put("logEntry", logEvent);
        properties.put("servlet", this);

        Event event = new Event(topic, properties);
        eventAdmin.postEvent(event);
    }

}
