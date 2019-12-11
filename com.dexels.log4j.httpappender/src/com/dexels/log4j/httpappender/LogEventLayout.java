package com.dexels.log4j.httpappender;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.spi.LoggingEvent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LogEventLayout {    
    private ObjectMapper mapper;
    private Map<String, String> vmArguments;
    private String application;
    private Long timeCorrection = 0L;

    public LogEventLayout() {
        mapper = new ObjectMapper();
    }

    public String format(LoggingEvent event) {
        StringWriter writer = new StringWriter();
        try {
            mapper.writeValue(writer, logToMap(event));
        } catch (IOException e) {
            // Something went wrong in converting the map to JSON. Very interesting?
        }
        return writer.toString();
    }

    @SuppressWarnings("deprecation")
    private Map<String, Object> logToMap(LoggingEvent event) {

        Map<String, Object> result = new HashMap<>();
        result.put("categoryName", event.categoryName);
        result.put("level", event.getLevel().toString());
        
        // Check if message is JSON. If it starts with a {, attempt to parse it.
        // If that fails, use regular string
        String msg = (String) event.getMessage();
        if (msg.startsWith("{")) {
            try {
                JsonNode jsonObject = mapper.readTree(msg);
                result.put("content", jsonObject);
            } catch (Exception e) {
                result.put("message", msg);
            }
        } else {
            result.put("message", msg);
        }
        
        result.put("timestamp", new Date(event.timeStamp + timeCorrection));
        if (timeCorrection != 0) {
            result.put("timeCorrection", timeCorrection);
        }

        if (event.getLocationInformation() != null) {
            Map<String, Object> locationMap = new HashMap<>();
            locationMap.put("className", event.getLocationInformation().getClassName());
            locationMap.put("fileName", event.getLocationInformation().getFileName());
            locationMap.put("lineNumber", event.getLocationInformation().getLineNumber());
            locationMap.put("methodName", event.getLocationInformation().getMethodName());
            result.put("location", locationMap);
        }
        
        String[] tp = event.getThrowableStrRep();
        try {
            if (tp != null && tp.length > 0) {
                int i = 0;

                StringBuffer sbuf = new StringBuffer();
                for (String line : tp) {
                    if (i != 0) {
                        sbuf.append("\t");
                    }
                    sbuf.append(line);
                    sbuf.append('\n');
                    i++;
                }
                String throwable = sbuf.toString();
                result.put("throwable", throwable);
            }
        } catch (Exception e) {
            // Failed to get a nice exception format - just use toString then
            result.put("throwable", event.getThrowableInformation().toString());
        }
        
        if (vmArguments != null) {
            result.put("vmarguments", vmArguments);
        }
        if (application != null) {
            result.put("application",  application);
        }

        result.put("mdc", event.getProperties());
        return result;
    }

    public void setVmArguments(Map<String, String> extraProps) {
        this.vmArguments = extraProps;
    }

    public void setApplication(String application) {
        this.application = application;
    }
    
    public void setTimeCorrection(Long correction) {
        this.timeCorrection  = correction;
    }
    
      
    public static void main(String[] args) {
        /*
         * USE FOR DEBUGGING
        
        LogEventLayout layout =new LogEventLayout();
        LoggingEvent event = new LoggingEvent(LogEventLayout.class.getName(), Category.getInstance(LogShipper.class), Priority.INFO, "TEST", null);
        String jsonResult = layout.format(event);
        
        System.out.println(jsonResult);
    
        
        event = new LoggingEvent(LogEventLayout.class.getName(), Category.getInstance(LogShipper.class), Priority.INFO, "TEST", new Exception("TEst Exception"));
        String jsonResult2 = layout.format(event);
        
        System.out.println(jsonResult2);
        
         */
    }
}
