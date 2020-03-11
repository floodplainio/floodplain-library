package com.dexels.kafka.streams.transformer.custom;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class SplitToListTransformer implements MessageTransformer {

	private static final String DEFAULT_SEPARATOR = " ";

    @Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
	    String field = params.get("from");
	    String to = params.get("to");
	    String separator = params.get("separator");
	    String toLower = params.get("toLower");
	    if (separator == null) {
	        separator = DEFAULT_SEPARATOR;
	    }
	    
	    Object value = msg.columnValue(field);
	    List<Object> result = new ArrayList<>();
	    
	    StringTokenizer st = new StringTokenizer(value.toString() ,separator);
	    while (st.hasMoreElements()) {
	        String token = st.nextToken();
	        if (token != null) {
	            if (toLower != null && toLower.equals("true")) {
	                token = token.toLowerCase();  
	            }
	            result.add(token);
	        }
	    }
	    return msg.with(to, result, "list");
	}

}
