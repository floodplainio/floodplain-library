package io.floodplain.streams.remotejoin;

import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;

public class IdentityProcessor extends AbstractProcessor<String, ReplicationMessage> {

    @Override
    public void process(String key, ReplicationMessage value) {
        super.context().forward(key, value);
    }

}
