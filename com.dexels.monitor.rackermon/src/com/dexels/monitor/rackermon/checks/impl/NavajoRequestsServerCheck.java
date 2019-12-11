package com.dexels.monitor.rackermon.checks.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.dexels.monitor.rackermon.checks.api.AbstractServerCheck;
import com.dexels.monitor.rackermon.checks.api.ServerCheckResult;

public class NavajoRequestsServerCheck extends AbstractServerCheck {
	private static final long serialVersionUID = -1333239223894290564L;
    private static final int EXCEPTIONS_ERROR_LIMIT_PERCENTAGE = 10;

	@Override
    public String getName() {
        return "Exceptions";
    }

    @Override
    public ServerCheckResult performCheck() {
        ServerCheckResult s = new ServerCheckResult(this);
        try {
            Optional<String> host = server.getEndPoint();
            if(!host.isPresent()) {
            	return new ServerCheckResult(this).setStatus(ServerCheckResult.STATUS_UNKNOWN);
            }
            Map<String, String> params = new HashMap<>();
            params.put("type", "requestcount");
            String response = performHttpPOSTCheck(host.get() + "/status", params);
            s.setRawResult(response);
            if (response.contains("error")) {
                s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            } else {
                String[] splitted = response.split("/");
                if (splitted.length < 1) {
                    s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
                } else {
                    Double exceptions = Double.valueOf(splitted[0]);
                    Double normalrequests = Double.valueOf(splitted[1]);

                    if ( normalrequests > 1 && ((exceptions / normalrequests) * 100) >= EXCEPTIONS_ERROR_LIMIT_PERCENTAGE) {
                        s.setStatus(ServerCheckResult.STATUS_ERROR);
                    }
                }

            }
        } catch (Exception e) {
            // something went wrong! Go to unknown state
            s.setStatus(ServerCheckResult.STATUS_UNKNOWN);
            s.setRawResult("Exception: " + e.getMessage());
        }

        return updateStatus(s);
    }

    @Override
    public int getMinimumFailedCheckCount() {
        return 6;
    }

  

}
