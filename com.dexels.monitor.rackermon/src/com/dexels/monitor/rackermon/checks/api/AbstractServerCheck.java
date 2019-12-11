package com.dexels.monitor.rackermon.checks.api;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.monitor.rackermon.Server;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.FormBody.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


public abstract class AbstractServerCheck implements ServerCheck {
	protected static final long serialVersionUID = 7699571923416122356L;
	private final static Logger logger = LoggerFactory.getLogger(AbstractServerCheck.class);
    private static final MediaType XML = MediaType.parse("application/xml;charset=utf-8");

    protected int minimumFailedCheckCount = 2;
    protected int runIntervalInSeconds = 120;
    protected String contentToCheck;

	private Map<Integer, OkHttpClient> clients = new HashMap<>();
	
    protected Server server;
    protected int statusCounter = 0;
    protected Date lastRun;
    
    @Override
    public void setServer(Server server){
        this.server = server;
    }
    
    
    @Override
    public void setParameters(int minimumFailedCheckCount, int runIntervalInSeconds, String contentToCheck) {
    	this.minimumFailedCheckCount = minimumFailedCheckCount;
    	this.runIntervalInSeconds = runIntervalInSeconds;
    	this.contentToCheck = contentToCheck;
    }
    
    protected int runIntervalInSeconds() {
        // By default, we run as often as our server wants to check us
        return 1;
    }
    
    /** Checks lastRun with runIntervalInSeconds() */
    @Override
    public boolean isTimeToRun() {
        if (lastRun == null) {
            return true;
        }
        Date now = new Date();
        return now.getTime() - lastRun.getTime() >  (runIntervalInSeconds() * 1000);
    }
    
   
    /* 
     * Depending on the amount of previous failures, this will set (or clear)
     * the ServerCheckResult.alertStatus
     */
    protected synchronized ServerCheckResult updateStatus(ServerCheckResult newStatus){
        if (newStatus.getStatus() > 0) {
            statusCounter++;
            if (statusCounter >= getMinimumFailedCheckCount() ) {
                if (statusCounter == getMinimumFailedCheckCount()) {
                    logger.info("Server {} has reached {} after {} consequtive failures", server.getName(), newStatus.getStatus(), statusCounter);
                }
                newStatus.setAlertStatus(newStatus.getStatus());
            } else {
                logger.info("Server {} has reached {} for {}, but I need {} more checks before I panic", server.getName(),
                        newStatus.getStatus(), this.getName(), (getMinimumFailedCheckCount() - statusCounter));
            }

        } else {
            statusCounter = 0;
            newStatus.setAlertStatus(newStatus.getStatus());
        }
        lastRun = new Date();
        return newStatus;
    }
    
    @Override
    public Server getServer() {
        return server;
    }
    
    @Override
    public Integer getServiceImpact() {
    	return 0;
    }
    
    @Override
    public int getClusterServiceFailureType() {
    	return CLUSTER_FAILURE_NONE;
    }
    
    
    @Override
    public String getFailureDescription() {
        return "genericfailure";
    }
    
    @Override
    public boolean unknownIsFailed() {
        return false;
    }
    
    @Override
    public String performHttpGETCheck(String url) {
        int timeout = getServer().getCheckTimeout();
        OkHttpClient client = getClient(timeout);
        
        Request request = new Request.Builder()
                .url(url)
                .build();
        
        try {
            Response response = client.newCall(request).execute();
            String result = response.body().string();
            if (response.code() >= 300) {
                logger.debug("Got responsecode {} connecting to {}: {}", response.code(), url, result);
                return "error: " + response.code();
            }
            return result;
        } catch (IOException e) {
            logger.warn("Got Exception on performing GET to {}: ",url,  e);
            return "error " + e.getMessage();
        }
    }
    
    @Override
    public String performHttpPOSTCheck(String url, Map<String, String> content) {
        OkHttpClient client = getClient(5);

        Builder formBodyBuilder = new FormBody.Builder();
        for (Entry<String, String> entry : content.entrySet()) {
            formBodyBuilder.add(entry.getKey(), entry.getValue());
        }
        FormBody formBody = formBodyBuilder.build();
        
        Request request = new Request.Builder()
                .url(url)
                .post(formBody)
                .build();
        
        try {
            Response response = client.newCall(request).execute();
            String result = response.body().string();
            if (response.code() >= 300) {
                logger.debug("Got responsecode {} connecting to {}: {}", response.code(), url, result);
                return "error: " + response.code();
            }
            return result;
        } catch (IOException e) {
            logger.warn("Got Exception on performing GET to {}: ",url,  e);
            return "error " + e.getMessage();
        }

    }
    
    @Override
    public String performHttpPOSTCheck(String url, String content, int timeout) {
        OkHttpClient client = getClient(timeout);

        RequestBody requestBody = RequestBody.create(XML, content);
        
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Content-ype", XML.toString())
                .addHeader("Accept", XML.toString())
                .addHeader("Accept-Encoding", "identity") // prevent adding Accept-Encoding: gzip header
                .post(requestBody)
                .build();
        
        try {
            Response response = client.newCall(request).execute();
            String result = response.body().string();
            if (response.code() >= 300) {
                logger.debug("Got responsecode {} connecting to {}: {}", response.code(), url, result);
                return "error: " + response.code();
            }
            return result;
        } catch (IOException e) {
            logger.warn("Got Exception on performing POST to {}: ",url,  e);
            return "error " + e.getMessage();
        }
    }

    private OkHttpClient getClient(Integer timeout) {
        if (!clients.containsKey(timeout)) {
            synchronized (clients) {
                OkHttpClient client = new OkHttpClient.Builder()
                        .connectTimeout(timeout, TimeUnit.SECONDS)
                        .readTimeout(timeout, TimeUnit.SECONDS)
                        .build();
                client.dispatcher().setMaxRequestsPerHost(100);
                clients.put(timeout, client);
            }
        }
        return clients.get(timeout);

    }
    
    

}
