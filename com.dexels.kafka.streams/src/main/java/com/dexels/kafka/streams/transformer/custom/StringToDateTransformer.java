package com.dexels.kafka.streams.transformer.custom;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class StringToDateTransformer implements MessageTransformer {
    private final static Logger logger = LoggerFactory.getLogger(StringToDateTransformer.class);

	@Override
	public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
		String field = params.get("field");
		String format = params.get("format");
		Object valueObj = msg.columnValue(field);
		if (valueObj instanceof Date) {
            // nothing to do, except if year is 9999
		    Calendar c = Calendar.getInstance();
		    c.setTime((Date) valueObj);
		    if( c.get(Calendar.YEAR)==9999) {
		        return msg.with(field, null, "date");
		    }
		    return msg;
        }
		
		String value = (String) valueObj;
		if(value == null || value.trim().equals("") || value.equals("9999-12-31")) {
		    return msg.with(field, null, "date");
		}
		
		SimpleDateFormat formatter;
		formatter = new SimpleDateFormat(format);
		Date parsed = null;
		try {
            parsed = formatter.parse(value);
        } catch (ParseException e) {
            logger.error("Parse exception in date {} (format {})", value, format, e);
        }
         
		return msg.with(field, parsed, "date");
		
	}

}
