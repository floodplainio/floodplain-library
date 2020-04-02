package com.dexels.kafka.streams.transformer.custom;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class CreateListTransformer implements MessageTransformer {

    @Override
    public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {
        String fields = params.get("from");
        String to = params.get("to");
        List<Object> result = new ArrayList<>();

        StringTokenizer st = new StringTokenizer(fields, ",");
        while (st.hasMoreElements()) {
            Object value = msg.columnValue(st.nextToken());
            if (value != null) {
                result.add(value);
            }
        }
        return msg.with(to, result, ImmutableMessage.ValueType.LIST);
    }

}
