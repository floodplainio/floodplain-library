package com.dexels.server.mgmt.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.server.mgmt.api.ServerStatisticsProvider;
import com.dexels.server.mgmt.api.ServerHealthProvider;
import com.dexels.server.mgmt.api.ServerHealthCheck;

@Component(name = "dexels.server.mgmt.servlet.status", service = { ServerHealthProvider.class, Servlet.class }, enabled = true, property = { "alias=/status",
        "servlet-name=status" }, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class StatusServlet extends HttpServlet implements ServerHealthProvider {
    private final static Logger logger = LoggerFactory.getLogger(StatusServlet.class);

    private static final long serialVersionUID = 1299175192263475554L;

    private Set<ServerHealthCheck> checks = new HashSet<>();
    private Map<String, ServerStatisticsProvider> statistics = new HashMap<>();
    private Set<String> requiredChecks = new HashSet<>();
    private Set<String> missingChecks = new HashSet<>();

    @Activate
    public void activate(Map<String, Object> settings) {
        if (settings.containsKey("requiredChecks")) {
            String requiredChecksString = (String) settings.get("requiredChecks");
            StringTokenizer st = new StringTokenizer(requiredChecksString, ",");
            while (st.hasMoreTokens()) {
                requiredChecks.add(st.nextToken());
            }

            missingChecks = new HashSet<>(requiredChecks);
            // Remove checks that are already bound
            synchronized (this) {
                checks.forEach((ServerHealthCheck c) -> {
                    missingChecks.remove(c.getClass().getSimpleName());
                });
            }

        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if (!isOk()) {
            String errorString = "";
            // Check for failed checks
            final StringBuilder failedChecks = new StringBuilder();
            checks.forEach((ServerHealthCheck c) -> {
                if (!c.isOk()) {
                    if (failedChecks.length() > 0) {
                        failedChecks.append("; ");
                    }
                    failedChecks.append(c.getDescription());
                }
            });

            // Check for missing checks
            final StringBuilder missing = new StringBuilder();
            missingChecks.forEach((String missingCheck) -> {
                if (missing.length() > 0) {
                    missing.append("; ");
                }
                missing.append(missingCheck);
            });

            if (missing.length() > 0) {
                errorString += "Missing checks: " + missing.toString();
            }
            if (missing.length() > 0 && failedChecks.length() > 0) {
                errorString += "\n";
            }
            if (failedChecks.length() > 0) {
                errorString += "Failed checks: " + failedChecks.toString();
            }

            resp.sendError(500, errorString);
            return;
        }

        resp.setContentType("text/plain");
        PrintWriter writer = resp.getWriter();
        writer.write("OK");
        writer.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String requestTpe = req.getParameter("type");
        String res = "";

        ServerStatisticsProvider s = statistics.get(requestTpe);
        if (s != null) {
            res = s.getValue();
        } else {
            logger.info("Requested unknown statistic: {}", requestTpe);
        }

        resp.setContentType("text/plain");
        PrintWriter writer = resp.getWriter();
        writer.write(res);
        writer.close();

    }

    @Override
    public boolean isOk() {
        if (!missingChecks.isEmpty()) {
            return false;
        }

        for (ServerHealthCheck c : checks) {
            if (!c.isOk()) {
                return false;
            }
        }
        return true;
    }

    @Reference(cardinality = ReferenceCardinality.MULTIPLE, unbind = "removeServerCheck", policy = ReferencePolicy.DYNAMIC)
    public void addServerCheck(ServerHealthCheck c) {
        synchronized (this) {
            missingChecks.remove(c.getClass().getSimpleName());
            checks.add(c);
        }

    }

    public void removeServerCheck(ServerHealthCheck c) {
        synchronized (this) {
            if (requiredChecks.contains(c.getClass().getSimpleName())) {
                missingChecks.add(c.getClass().getSimpleName());
            }
            checks.remove(c);
        }
    }

    @Reference(cardinality = ReferenceCardinality.MULTIPLE, unbind = "removeServerStatisticsProvider", policy = ReferencePolicy.DYNAMIC)
    public void addServerStatisticsProvider(ServerStatisticsProvider s) {
        statistics.put(s.getKey(), s);
    }

    public void removeServerStatisticsProvider(ServerStatisticsProvider s) {
        statistics.remove(s.getKey());
    }

}