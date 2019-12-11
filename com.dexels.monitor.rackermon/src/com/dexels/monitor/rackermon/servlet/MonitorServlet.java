package com.dexels.monitor.rackermon.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.kubernetes.UpdateClusterMembers;
import com.dexels.monitor.rackermon.Cluster;
import com.dexels.monitor.rackermon.ClusterAlert;
import com.dexels.monitor.rackermon.Group;
import com.dexels.monitor.rackermon.Server;
import com.dexels.monitor.rackermon.alert.Alerter;
import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.persistence.Persistence;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component(name = "dexels.monitor.servlet", service = { Servlet.class}, enabled = true, property = {
        "alias=/monitorapi", "servlet-name=monitorapi"})
public class MonitorServlet extends HttpServlet {

    private static final long serialVersionUID = -1948354354961917987L;
	private final static Logger logger = LoggerFactory.getLogger(MonitorServlet.class);
	
	private List<Group> myGroups = new ArrayList<>();
	private SimpleDateFormat dateformatter = null;
    private ObjectMapper mapper;
    private Alerter alerter;
	private Persistence persistence;
    private UpdateClusterMembers updateClient;
	
	@Activate
	public void activate(BundleContext context) {
        try {
			mapper = new ObjectMapper();
			dateformatter = new SimpleDateFormat("dd-MM HH:mm:ss");
			logger.info("MonitorServlet activated");
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
	}

	@Override
	protected void service( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
	    String query = request.getParameter("query");
	    
	    // Create session to make us sticky for loadbalancer
	    request.getSession();
	    
	    
	    String result = "[]";
	    if (query.equals("globalstatus")) {
	        Map<String, List<Group>> resultMap = new HashMap<>();
	        resultMap.put("groups", myGroups);
	        result = mapper.writeValueAsString(resultMap);
	    } else  if (query.equals("shortstatus")) {
	        
	        Map<String, Map<String, List<String>>> resultMap = new HashMap<>();
            Map<String, List<String>> groupMap = new HashMap<>();
            for (Group g : myGroups) {
              
                for (Cluster c : g.getClusters()) {
                    if (c.getPriority() < 1) {
                        continue;
                    }
                    Set<ServerCheck> failedChecks = c.getServerCheckFailures(); 
                    List<String> failures = new ArrayList<>();
                    if (failedChecks.size() > 0) {
                      
                        for (ServerCheck check : failedChecks ) {
                            failures.add(check.getServer().getName() + " " + check.getName());
                        }
                    } 
                    groupMap.put(c.getName(), failures);
                }
            }
            resultMap.put("groups", groupMap);

            
            result = mapper.writeValueAsString(resultMap);
	        
	        
	    } else  if (query.equals("alerts")) {
            Map<String, List<String>> resultMap = new HashMap<>();
            if (alerter != null) {
                Map<Object, Object> alerts = alerter.getPreviousAlerts();
                SortedSet<Object> keys = new TreeSet<Object>(java.util.Collections.reverseOrder());
                keys.addAll(alerts.keySet());
                List<String> alertStrings = new ArrayList<>();
                for (Object key : keys) {
                    Date d = (Date) key;
                    String content = (String) alerts.get(key);
                    alertStrings.add(dateformatter.format(d) + " - " + content);
                }
                resultMap.put("alerts", alertStrings);
            }
            
            result = mapper.writeValueAsString(resultMap);
	    } else  if (query.equals("statusoverview")) {
	        /* Used for the status overview */
            Map<String,  Map<String, String>> resultMap = new HashMap<>();
            Map<String, String> clusterMap = new HashMap<>();
            for (Group g : myGroups) {
              
                for (Cluster c : g.getClusters()) {
                    if (c.getImpactingServerCheckFailure() != null) {
                        clusterMap.put(c.getName(), c.getImpactingServerCheckFailure().getFailureDescription());
                    } else {
                        clusterMap.put(c.getName(), "OK");
                    }
                }
            }
            resultMap.put("cluster", clusterMap);

            
            result = mapper.writeValueAsString(resultMap);
	    } else  if (query.equals("historicstatusoverview")) {
	        /* Used for the status overview */
            Map<String,  List<ClusterAlert>> resultMap = new HashMap<>();
            for (Group g : myGroups) {
              
                for (Cluster c : g.getClusters()) {
                    List<ClusterAlert> alerts = persistence.getHistoricClusterAlerts(c.getName());
                    resultMap.put(c.getName(),  alerts);
                }
            }
            
            result = mapper.writeValueAsString(resultMap);
       
	    } else  if (query.equals("uptime")) {
	        /* Used for the status overview */
            Map<String, String> resultMap = new HashMap<>();
            for (Group g : myGroups) {
              
                for (Cluster c : g.getClusters()) {
                    resultMap.put(c.getName(),   persistence.getUptime(c.getName()));
                }
            }
            
            result = mapper.writeValueAsString(resultMap);
	    } else if(query.equals("maintenance")) {
	        String hostname = request.getParameter("hostname");
	        String start = request.getParameter("start");
	        boolean found = false;
	        for (Group g : myGroups) {
                for (Cluster c : g.getClusters()) {
                    for (Server s : c.getServers()) {
                        if (s.getEndPoint().equals(hostname)) {
                            found = true;
                            if ( start.equals("true")) {
                                s.setMaintenance(true);
                            } else {
                                s.setMaintenance(false);
                            }
                            break;
                        }
                    }
                }
            }
	        if (!found) {
	            logger.warn("Unable to find host to update maintenance. Hostname: {} start: {}", hostname, start);
	        }
        } else if (query.equals("restart")) {
            String cluster = request.getParameter("cluster");
            String namespace = request.getParameter("namespace");
            String pods = request.getParameter("pods");
            int timeout = Integer.parseInt(request.getParameter("timeout"));
            logger.info("Restarting pods " + pods + " in " + cluster + " with timeout " + String.valueOf(timeout) + " in " + namespace);
            updateClient.rollingRestartDeploymentByLabelSelector(namespace, "release=" + cluster, timeout);
        } else {
	       logger.warn("Unsupported operation on MonitorServlet: {}", query);
	    }
	    response.setContentType("text/plain");
        PrintWriter writer = response.getWriter();
        writer.write(result);
        writer.close();
	}

    
    @Reference(policy=ReferencePolicy.DYNAMIC, unbind="removeGroup", cardinality=ReferenceCardinality.AT_LEAST_ONE)
    public void addGroup(Group group) {
        myGroups.add(group);
    }

    public void removeGroup(Group group) {
        myGroups.remove(group);
    }
    
    @Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearAlerter", cardinality=ReferenceCardinality.OPTIONAL)
    public void setAlerter(Alerter alerter) {
        this.alerter = alerter;
    }

    public void clearAlerter(Alerter group) {
        this.alerter = null;
    }
    
    @Reference(policy = ReferencePolicy.DYNAMIC, name = "Persistence", unbind = "clearPersistence")
    public void setPersistence(Persistence p) {
        this.persistence = p;
    }

    @Reference()
    public void setUpdateClusterMembers(UpdateClusterMembers up) {
        updateClient = up;
    }

    public void clearPersistence(Persistence p) {
        this.persistence = null;
    }
	
	
}