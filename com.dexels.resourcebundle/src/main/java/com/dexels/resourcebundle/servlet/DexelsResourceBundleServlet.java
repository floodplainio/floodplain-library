package com.dexels.resourcebundle.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.resourcebundle.ResourceBundleStore;

@Component(name = "dexels.resourcebundle.servlet", service = { Servlet.class}, enabled = true, property = { "alias=/resourcebundle", "servlet-name=resourcebundle" })
public class DexelsResourceBundleServlet extends HttpServlet{
   
    private static final long serialVersionUID = 1394216681768340208L;
    private final static Logger logger = LoggerFactory.getLogger(DexelsResourceBundleServlet.class);

    private ResourceBundleStore dResourceBundle = null;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String application = request.getParameter("application");
        String resource = request.getParameter("resource");
        String union = request.getParameter("union");
        String subunion = request.getParameter("subunion");
        String locale = request.getParameter("locale");
        
        response.setHeader("Connection",  "close");
        try {
            String resourceText = dResourceBundle.getResource(application, resource, union, subunion, locale);
            response.setContentType("text/plain; charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.write(resourceText);
            writer.close();
        } catch (Throwable t) {
            logger.error("Error in handling resourcebundle request!", t);
            response.setStatus(500);
        }
    }


    @Reference(unbind = "clearResourceBundleStore", policy = ReferencePolicy.DYNAMIC)
    public void setResourceBundleStore(ResourceBundleStore b) {
        this.dResourceBundle = b;
    }

    public void clearResourceBundleStore(ResourceBundleStore a) {
        this.dResourceBundle = null;
    }


}
