package com.dexels.kafka.streams.remotejoin;

import org.apache.kafka.streams.processor.AbstractProcessor;

import com.dexels.replication.api.ReplicationMessage;

public class PreJoinProcessor extends AbstractProcessor<String, ReplicationMessage> {
    public static String REVERSE_IDENTIFIER = "_REV_";
    
    
    private boolean isReverseJoin;

    public PreJoinProcessor(boolean isReverseJoin) {
        this.isReverseJoin = isReverseJoin;
    }

    @Override
    public void process(String key, ReplicationMessage msg) {
        if (isReverseJoin) {
            String newKey = key;
            newKey += REVERSE_IDENTIFIER;
            context().forward(newKey, msg == null ? null : msg.withoutParamMessage());
        } else {
            context().forward(key, msg == null ? null : msg.withoutParamMessage());
        }
        
    }

}
