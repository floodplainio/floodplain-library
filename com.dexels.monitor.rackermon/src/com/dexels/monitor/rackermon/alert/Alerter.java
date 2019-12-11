package com.dexels.monitor.rackermon.alert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.monitor.rackermon.Cluster;
import com.dexels.monitor.rackermon.Group;
import com.dexels.monitor.rackermon.Server;
import com.dexels.monitor.rackermon.checks.api.ServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;

@Component(name = "dexels.monitor.alerter", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, property = {
        "event.topics=monitor/statechange" }, service = { Alerter.class })
public class Alerter implements Runnable, EntryListener<Object, Object> {
    
    private static final String RACKERMON_MAIL_SUBJECT = "[RackerMon] Status updates!";

    public static final String TOPIC = "monitor/alert";

    private final static Logger logger = LoggerFactory.getLogger(Alerter.class);
    private static final long DEFAULT_CHECK_INTERVAL = 60000;

    private static final int MAX_ALERTS = 20;

    private int maxAlertsPerTimeFrame = 5;
    private long timeFrame = 60000;
    private long timeMillis = 0;
    private int alertsPerTimeFrame = 0;
    private boolean notifiedMaximumReached = false;

    private HazelcastService hazelcastService;
    private List<Group> myGroups = new ArrayList<>();
    private IMap<Object, Object> previousAlerts;
    private Map<String, Integer> sentAlerts = new HashMap<>();
    
    private TopicPublisher topicPublisher;

    private Long runFrequency = DEFAULT_CHECK_INTERVAL;

    private String hostname;
    private String username;
    private String password;
    private String emailRecipients;
    private boolean keepRunning;
    private String slackTopic;

    private boolean enableTimeFrame = false;

    @Activate
    public void activate(Map<String, Object> settings) {
        keepRunning = true;
        runFrequency = Long.valueOf((String) settings.get("runFrequency"));
        hostname = (String) settings.get("hostname");
        username = (String) settings.get("username");
        password = (String) settings.get("password");
        emailRecipients = (String) settings.get("emailTo");
        
        if (settings.containsKey("enableTimeFrame")) {
            enableTimeFrame = Boolean.valueOf(settings.get("enableTimeFrame").toString());
            this.timeMillis = System.currentTimeMillis();
            this.timeFrame = Long.valueOf(settings.get("timeFrame").toString());
            this.maxAlertsPerTimeFrame = Integer.valueOf(settings.get("maxAlertsPerTimeFrame").toString());
        }
        
        if (settings.containsKey("slackTopic")) {
            this.slackTopic = (String) settings.get("slackTopic");
        }
        
        previousAlerts = (IMap<Object, Object>) hazelcastService.getMap("previousAlerts");
        previousAlerts.addEntryListener(this, false);
        (new Thread(this)).start();
    }
    
    @Modified
    public void modified(Map<String, Object> settings) {
        runFrequency = Long.valueOf((String) settings.get("runFrequency"));
        hostname = (String) settings.get("hostname");
        username = (String) settings.get("username");
        password = (String) settings.get("password");
        emailRecipients = (String) settings.get("emailTo");
    }

    @Deactivate
    public void deactivate() {
        keepRunning = false;
    }

    
    @Reference(policy=ReferencePolicy.DYNAMIC, unbind="removeGroup", cardinality=ReferenceCardinality.AT_LEAST_ONE)
    public void addGroup(Group group) {
        myGroups.add(group);
    }

    public void removeGroup(Group group) {
        myGroups.remove(group);
    }

    @Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearTopicPublisher",target="(component.name=dexels.slack.publisher)", cardinality=ReferenceCardinality.OPTIONAL)
    public void setTopicPublisher(TopicPublisher topicPublisher) {
        this.topicPublisher = topicPublisher;
    }

    public void clearTopicPublisher(TopicPublisher topicPublisher) {
        this.topicPublisher = null;
    }

    @Override
    public void run() {
        logger.info("Starting alerter thread!");

        while (keepRunning) {
            try {
                if (isOldestMember()) {
                    Map<ServerCheck, ServerCheckResult> clusterFailures = new HashMap<>();
                    List<Group> groupsToCheck = new ArrayList<>(myGroups);
                    for (Group g : groupsToCheck) {
                        for (Cluster c : g.getClusters()) {
                            clusterFailures.putAll(c.getServerCheckResultFailures());
                        }
                    }
                    checkAndProcessFailures(clusterFailures);
                }
                try {
                    Thread.sleep(runFrequency);
                } catch (InterruptedException e) {
                    logger.warn("Got InterruptedException - stopping thread");
                    keepRunning = false;
                }
            } catch (Throwable t) {
            	t.printStackTrace();
                logger.error("Error in alerter thread!", t);
                try {
                	logger.info("Error back off");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
           
        }
        logger.warn("Stopping thread in Alerter - no more alerts!");
    }


    private void checkAndProcessFailures(Map<ServerCheck, ServerCheckResult> clusterFailures) {
        Map<String, Integer> leftOverAlerts = new HashMap<>(sentAlerts);
        Map<String, String> newAlerts = new HashMap<>();
        Map<String, String> existingAlerts = new HashMap<>();

        for (ServerCheck servercheck : clusterFailures.keySet()) {
            Server s = servercheck.getServer();
            ServerCheckResult result = clusterFailures.get(servercheck);
            
            if (s.getPriority() < 1) {
                logger.debug("Server {}  has priority {} - no alert needed!", s.getName(), s.getPriority());
                continue;
            }
            if (s.getMaintenanceMode()) {
                logger.warn("Server {} is in maintenance mode - ignoring alert!", s.getName());
                continue;
            }

            String key = s.getName() + "__" + servercheck.getName();
            String description = describeServerCheckStatus(servercheck, result);
            if (!sentAlerts.containsKey(key)) {
                // This is a new alert we didn't report about yet
                sentAlerts.put(key, result.getStatus()); 
                previousAlerts.put(new Date(), description);
                newAlerts.put(key, description);
            } else {
                // check if the new status is different.
                if (sentAlerts.get(key) == result.getStatus()) {
                    existingAlerts.put(key,  description);
                } else {
                    sentAlerts.put(key, result.getStatus() );
                    newAlerts.put(key, description);
                }
            }
            leftOverAlerts.remove(key);
        }
        for (String composedKey : leftOverAlerts.keySet()) {
            String[] splitted = composedKey.split("__");
            previousAlerts.put(new Date(), splitted[0] + " " + splitted[1]+ " back to normal");
            sentAlerts.remove(composedKey);
        }
        publishNotifications(newAlerts,existingAlerts,  leftOverAlerts);
    }

    private void publishNotifications(Map<String, String> newAlerts, Map<String, String> existingAlerts,
            Map<String, Integer> leftOverAlerts) {
        
        
        if (newAlerts.isEmpty() && leftOverAlerts.isEmpty()) {
            // Nothing to report
            return;
        }
        StringBuilder alertBody = new StringBuilder();
        if (newAlerts.keySet().size() > 0) {
            alertBody.append("New Problems:\n");
            for (String description: newAlerts.values()) {
                alertBody.append(description);
                alertBody.append("\n");
            }
            alertBody.append("\n\n");
        }

        // Any elements left in leftOverAlerts, are actually no longer in alert right now
        if (leftOverAlerts.size() > 0) {
            alertBody.append("Cleared:\n");
            for (String composedKey : leftOverAlerts.keySet()) {
                String[] splitted = composedKey.split("__");
                alertBody.append(splitted[0]);
                alertBody.append(": ");
                alertBody.append(splitted[1]);
                alertBody.append("\n");
            }
            alertBody.append("\n\n");
        }
        
        if (existingAlerts.keySet().size() > 0) {
            alertBody.append("Existing Problems:\n");
            for (String description: existingAlerts.values()) {
                alertBody.append(description);
                alertBody.append("\n");
            }
            alertBody.append("\n\n");
        }
        
        if (alertBody.length() > 0) {
            sendEmailImpl(alertBody.toString());
            
            if (enableTimeFrame && !allowPublishToSlackTopic()) {
                logger.info("Maximum alerts per hour reached. Will not publish alert to slack.");
                // Publish that the maximum has been reached ONLY ONCE
                if(!this.notifiedMaximumReached) {
                    try {
                        String message = "Blocking publishing for " + (int) (this.timeFrame/60);
                        topicPublisher.publish(slackTopic, "", message.getBytes());
                        this.notifiedMaximumReached = true;
                    } catch (IOException e) {
                        logger.error("Error publishing msg: {}", e);
                    }
                }
                return;
            }

            // Publish to slackTopic
            if (topicPublisher == null || slackTopic == null) {
                return;
            }
            try {
                topicPublisher.publish(slackTopic, "", alertBody.toString().getBytes());
            } catch (IOException e) {
                logger.error("Error publishing msg: {}", e);
            }
        }
 
    }

    private boolean allowPublishToSlackTopic() {

        // If TIME_FRAME has passed since the time that the timeMillis was set, then
        // init again and allow publishing
        if (System.currentTimeMillis() - this.timeMillis > timeFrame) {
            this.timeMillis = System.currentTimeMillis();
            this.alertsPerTimeFrame = 1;
            this.notifiedMaximumReached = false;
            return true;
        } else if (this.alertsPerTimeFrame < maxAlertsPerTimeFrame) {
            // If TIME_FRAME has not passed, and alertsPerTimeFrame <
            // MAX_ALERTS_PER_TIMEFRAME increase the alertsPerTimeFrame and allow publishing
            this.alertsPerTimeFrame++;
            return true;
        }
        // Else, do not allow publishing
        return false;
    }

    private String describeServerCheckStatus(ServerCheck servercheck, ServerCheckResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(servercheck.getServer().getName());
        builder.append(": ");
        builder.append(servercheck.getName());
        builder.append(" reached ");
        builder.append(result.getRawResult());
        builder.append(" (");
        builder.append(translateStatus(result.getAlertStatus()));
        builder.append(")");
        return builder.toString();
    }
    

    private void sendEmailImpl(String contents) {
        logger.info(contents);
        Properties properties = System.getProperties();
        properties.setProperty("mail.smtp.host", hostname);
        properties.setProperty("mail.smtp.auth", "true");
        Session session = Session.getInstance(properties, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });

        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress("monitor@mailgun.sportlink.nl"));
            
            StringTokenizer tok = new StringTokenizer(emailRecipients, ",");
            List<InternetAddress> addresses = new ArrayList<>();
            while (tok.hasMoreTokens()) {
                addresses.add(new InternetAddress( tok.nextToken()));
            }
            message.addRecipients(Message.RecipientType.TO, addresses.toArray(new InternetAddress[]{}));
            
            message.setSubject(RACKERMON_MAIL_SUBJECT);
            message.setText(contents);
            Transport.send(message);
        } catch (Exception e) {
            logger.error("Exception on sending email! {}", e);
        }

    }

    private String translateStatus(Integer status) {
        if (status == ServerCheckResult.STATUS_ERROR) {
            return "ERROR";
        }
        if (status == ServerCheckResult.STATUS_OK) {
            return "OK";
        }
        if (status == ServerCheckResult.STATUS_WARNING) {
            return "WARNING";
        }

        return "UNKNOWN";

    }


    @Reference(name = "HazelcastService", unbind = "clearHazelcastService", policy=ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService service) {
        this.hazelcastService = service;
    }

    public void clearHazelcastService(HazelcastService service) {
        this.hazelcastService = null;
    }

    public Map<Object, Object> getPreviousAlerts() {
        return previousAlerts;
    }
    
    private boolean isOldestMember() {
        return hazelcastService.oldestMember().equals(hazelcastService.getHazelcastInstance().getCluster().getLocalMember());
    }
  

    @Override
    public void entryAdded(EntryEvent<Object, Object> arg0) {
        // If the collection gets too large, remove the oldest one
        if (previousAlerts.keySet().size() > MAX_ALERTS) {
            Set<Object> keys = previousAlerts.keySet();
            List<Date> sortedKeys = new ArrayList<>();

            for (Object d : keys) {
                sortedKeys.add((Date) d);
            }

            Collections.sort(sortedKeys);
            previousAlerts.delete(sortedKeys.iterator().next());
        }
        
    }
    


    @Override
    public void entryEvicted(EntryEvent<Object, Object> arg0) { 
    }

    @Override
    public void entryRemoved(EntryEvent<Object, Object> arg0) {
    }

    @Override
    public void entryUpdated(EntryEvent<Object, Object> arg0) {
    }

	@Override
	public void mapCleared(MapEvent arg0) {
	}

	@Override
	public void mapEvicted(MapEvent arg0) {
	}

    
}
