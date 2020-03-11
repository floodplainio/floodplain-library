package com.dexels.kafka.streams.transformer.custom;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.util.Map;

public class CopyFieldTransformer implements MessageTransformer {


    @Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
	    String field = params.get("from");
	    String to = params.get("to");
	    String transform = params.get("transform");

	    
	    Object value = msg.columnValue(field);
	    Object newValue = value;
	    if (transform != null && value instanceof String) {
	    	if (transform.equals("toLower")) {
	    		newValue = ( (String) value).toLowerCase();
	    	} else if (transform.equals("toLower")) {
	    		newValue = ( (String) value).toUpperCase();
	    	} else {
	    		// unknown transformer!
	    	}
	    }
	    
	    
	    return msg.with(to, newValue, msg.columnType(field));
	}

}
