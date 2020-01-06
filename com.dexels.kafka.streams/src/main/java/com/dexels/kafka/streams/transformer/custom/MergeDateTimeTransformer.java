package com.dexels.kafka.streams.transformer.custom;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class MergeDateTimeTransformer implements MessageTransformer {

    @Override
    public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
        String field1 = params.get("fromdate");
        String field2 = params.get("fromtime");
        String to = params.get("to");

        if (field1 == null || field2 == null) {
            return msg;
        }
        if (to == null) {
            to = "datetime";
        }

        Date field1Value = (Date) msg.columnValue(field1);
        Date field2Value = (Date) msg.columnValue(field2);

        if (field1Value == null || field2Value == null) {
            return msg;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(field1Value);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(field2Value);

        cal.set(Calendar.HOUR_OF_DAY, cal2.get(Calendar.HOUR_OF_DAY));
        cal.set(Calendar.MINUTE, cal2.get(Calendar.MINUTE));
        cal.set(Calendar.SECOND, cal2.get(Calendar.SECOND));

        return msg.with(to, cal.getTime(), "date");

    }

}
