package com.dexels.monitor.rackermon.checks.impl;

import java.io.IOException;
import java.net.URL;
import java.util.Date;

import org.apache.commons.io.IOUtils;

import com.dexels.monitor.rackermon.checks.api.AbstractServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class NavajoBirtServerCheck extends AbstractServerCheck {
	private static final long serialVersionUID = 1224418578662471022L;
	
	private transient String requestNavajo;
    
    public NavajoBirtServerCheck() {
        getRequestNavajo();
    }

    @Override
    public String getName() {
        return "Birt";
    }
    

    @Override
    public ServerCheckResult performCheck() {  
        // For long running checks, prevent duplicate runs
        lastRun = new Date();
        
        ServerCheckResult s = new ServerCheckResult(this);

        if (requestNavajo == null || requestNavajo.equals("")) {
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            s.setRawResult("No input doc found!");
            return s;
        }
        String response = performHttpPOSTCheck(server.getEndPoint() + "/navajobirt/Postman", requestNavajo, 30);
        s.setRawResult(response);
        if (response.length() > 20) {
            s.setRawResult(response.substring(0, 20) + "...");
        } 
        
        if (response.contains("error")) {
            s.setStatus(ServerCheckResult.STATUS_ERROR);
        }
        if (response.contains("Result")) {
            s.setStatus(ServerCheckResult.STATUS_OK);
            s.setRawResult("OK");
        } else {
            s.setStatus(ServerCheckResult.STATUS_ERROR);
        }

        return updateStatus(s);
    }

    @Override
    public int getMinimumFailedCheckCount() {
        return 1;
    }
    
    @Override
    public int runIntervalInSeconds() {
        return 300;
    }
    private void getRequestNavajo() {
        try {
            URL url = getClass().getResource("birtnavajo.xml");
            String content = IOUtils.toString(url.openStream(), "utf-8");
            requestNavajo = content;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
