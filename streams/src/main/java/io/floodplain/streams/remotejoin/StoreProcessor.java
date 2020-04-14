package io.floodplain.streams.remotejoin;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class StoreProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final String lookupStoreName;
    private KeyValueStore<String, ReplicationMessage> lookupStore;

    public StoreProcessor(String lookupStoreName) {
        this.lookupStoreName = lookupStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(lookupStoreName);
        super.init(context);
    }

    @Override
    public void close() {

    }

    @Override
    public void process(String key, ReplicationMessage outerMessage) {
        if (outerMessage == null || outerMessage.operation() == Operation.DELETE) {
            ReplicationMessage previous = lookupStore.get(key);
//			logger.info("Delete detected in store: {} with key: {}",lookupStoreName,key);
            if (previous != null) {
                lookupStore.delete(key);
                context().forward(key, previous.withOperation(Operation.DELETE));
            }
            context().forward(key, null);
        } else {
            lookupStore.put(key, outerMessage);
            context().forward(key, outerMessage);
        }
    }

}
