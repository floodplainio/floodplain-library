package com.dexels.monitor.rackermon.checks.impl;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.monitor.rackermon.checks.api.AbstractHttpCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class HttpCheck extends AbstractHttpCheck{
    private static final long serialVersionUID = 268788234586571418L;
    private final static Logger logger = LoggerFactory.getLogger(HttpCheck.class);

    @Override
    public String getName() {
        return "http";
    }

    @Override
    public ServerCheckResult performCheck() {
        ServerCheckResult s = new ServerCheckResult(this);
        try {
            Optional<String> host = server.getEndPoint();
            if(!host.isPresent()) {
            	return new ServerCheckResult(this).setStatus(ServerCheckResult.STATUS_UNKNOWN);
            }
//            String host = server.getEndPoint();
            String response = performHttpGETCheck(host.get());
            s.setRawResult(response);

            if (response.startsWith("error")) {
                s.setStatus(ServerCheckResult.STATUS_ERROR);
            } else {
                s.setRawResult("OK");
                s.setStatus(ServerCheckResult.STATUS_OK);
                if (contentToCheck != null) {
                    if (!response.contains(contentToCheck)) {
                        logger.debug("Expected {}, but got: {}",contentToCheck, response );
                        s.setRawResult("Unexpected content");
                        s.setStatus(ServerCheckResult.STATUS_ERROR);
                    }
                }
            }
        } catch (Exception e) {
            // something went wrong! Go to unknown state
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            s.setRawResult("Exception");
        }

        return updateStatus(s);
    
    }

}
