package com.dexels.pax.logging.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;

import org.ops4j.pax.logging.spi.PaxAppender;
import org.ops4j.pax.logging.spi.PaxLoggingEvent;

import com.dexels.redis.client.RedisClient;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class PaxRedisAppender implements PaxAppender {

    private static final String DEFAULT_STATS_KEY = "stats";

    private volatile HeartBeatThread heartBeatThread;

    private PaxJSONEventLayout layout;

    // logger configurable options
    private String key = null;
    private String password = null;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private int database = Protocol.DEFAULT_DATABASE;

    private final RedisClient redisClient;

    public PaxRedisAppender(RedisClient redis) {
        layout = new PaxJSONEventLayout();
        this.redisClient = redis;
        String sourceHost = System.getenv("HOSTNAME");
        if (sourceHost == null || sourceHost.trim().equals("")) {
            try {
                sourceHost = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                sourceHost = "unknown";
            }
        }
        setSourceHost(sourceHost);
    }

    @Override
    public void doAppend(PaxLoggingEvent event) {
        try(Jedis client = redisClient.getPool().getResource()) {
            String json = layout.doLayout(event);
            if (event.getLoggerName().equals("stats")) {
                client.rpush(DEFAULT_STATS_KEY, json);
            } else {
                client.rpush(key, json);
            }
            stopHeartBeatThread();
        } catch (Exception e) {
            e.printStackTrace();
            startHeartbeatThread(event);
        }
    }
  
    public void doUnsafeAppend(PaxLoggingEvent event) throws Exception {
        try(Jedis client = redisClient.getPool().getResource()) {
            String json = layout.doLayout(event);
            if (event.getLoggerName().equals("stats")) {
                client.rpush(DEFAULT_STATS_KEY, json);
            } else {
                client.rpush(key, json);
            }
            stopHeartBeatThread();
        } 
    }
    


    private void stopHeartBeatThread() {
        if (heartBeatThread != null) {
        	final HeartBeatThread t = heartBeatThread;
            synchronized(this) {
                t.keepRunning = false;
                heartBeatThread = null;
            }
         
        }
    }

    private void startHeartbeatThread(PaxLoggingEvent event) {

        if (heartBeatThread == null) {
            synchronized(this) {
                heartBeatThread = new HeartBeatThread(this, event);
                Thread th = new Thread(heartBeatThread);
                th.start();
            }
        } else {
            // Skipping this event since the heartBeatThread is already running
        }

    }

    public String getSource() {
        return layout.getSource();
    }

    public void setSource(String source) {
        layout.setSource(source);
    }

    public String getSourceHost() {
        return layout.getSourceHost();
    }

    public void setSourceHost(String sourceHost) {
        System.err.println("Using source host for redis appender: " + sourceHost);
        layout.setSourceHost(sourceHost);
    }

    public String getSourcePath() {
        return layout.getSourcePath();
    }

    public void setSourcePath(String sourcePath) {
        layout.setSourcePath(sourcePath);
    }

    public String getTags() {
        if (layout.getTags() != null) {
            Iterator<String> i = layout.getTags().iterator();
            StringBuffer sb = new StringBuffer();
            while (i.hasNext()) {
                sb.append(i.next());
                if (i.hasNext()) {
                    sb.append(',');
                }
            }
            return sb.toString();
        }
        return null;
    }

    public void setTags(String tags) {
        if (tags != null) {
            String[] atags = tags.split(",");
            layout.setTags(Arrays.asList(atags));
        }
    }

    public String getType() {
        return layout.getType();
    }

    public void setType(String type) {
        layout.setType(type);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setMdc(boolean flag) {
        layout.setProperties(flag);
    }

	public void setDeDotKeys(boolean flag) {
		layout.setDeDotKeys(flag);
	}
    public boolean getMdc() {
        return layout.getProperties();
    }

    public void setLocation(boolean flag) {
        layout.setLocationInfo(flag);
    }

    public boolean getLocation() {
        return layout.getLocationInfo();
    }

    public void setCallerStackIndex(int index) {
        layout.setCallerStackIdx(index);
    }

    public int getCallerStackIndex() {
        return layout.getCallerStackIdx();
    }

    public void setSourceInstance(String sourceInstance) {
        layout.setSourceInstance(sourceInstance);
    }

    public void setSourceContainer(String sourceContainer) {
        layout.setSourceContainer(sourceContainer);
    }

    public class HeartBeatThread implements Runnable {
        private static final int HEARTBEAT_SLEEP = 15000;
        private PaxRedisAppender appender;
        protected volatile boolean keepRunning = true;
        private PaxLoggingEvent originalEvent;
        
        public HeartBeatThread(PaxRedisAppender appender, PaxLoggingEvent event) {
            this.appender = appender;
            this.originalEvent = event;
        }

        public void run() {
            while (keepRunning) {
                try {
                    try {
                        appender.doUnsafeAppend(originalEvent);
                        keepRunning = false;
                    } catch (Exception e) {
                        System.out.println("Error in PaxRedisAppender.append(). Going to re-create pool and try again in a bit...");
                        appender.redisClient.rebuild();
//                        appender.stop();
//                        appender.start();
                    }
                   
                    Thread.sleep(HEARTBEAT_SLEEP);
                } catch (InterruptedException e) {
                    keepRunning = false;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
