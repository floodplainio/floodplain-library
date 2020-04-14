package io.floodplain.streams.remotejoin.ranged;

import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class GroupedUpdateProcessor extends AbstractProcessor<String, ReplicationMessage> {
    private final String mappingStoreName;
    private final String lookupStoreName;

    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private KeyValueStore<String, ReplicationMessage> mappingStore;
    private final Function<ReplicationMessage, String> keyExtract;
    private final boolean ignoreOriginalKey;
    private final boolean log;


    private final static Logger logger = LoggerFactory.getLogger(GroupedUpdateProcessor.class);


    public GroupedUpdateProcessor(String lookupStoreName, Function<ReplicationMessage, String> keyExtract, String mappingStoreName, boolean ignoreOriginalKey) {
        this.lookupStoreName = lookupStoreName;
        this.mappingStoreName = mappingStoreName;
        this.keyExtract = keyExtract;
        this.ignoreOriginalKey = ignoreOriginalKey;
        this.log = false

        ;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(lookupStoreName);
        this.mappingStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(mappingStoreName);
        super.init(context);
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public void process(String key, ReplicationMessage msg) {
        if (msg != null) {
            String assembled = assembleGroupedKey(key, msg);
            if (log) {
                logger.info("Processor: {}, Assembling key. Ignoring original: {}, original: {} assembled: {}", lookupStoreName, ignoreOriginalKey, key, assembled);
            }
            if (msg.operation() == Operation.DELETE) {
                lookupStore.delete(assembled);
                mappingStore.delete(key);
            } else {
                // Check if we have a previous mapping
                ReplicationMessage previousVersion = mappingStore.get(key);
                if (previousVersion != null) {
                    String previousAssembled = ignoreOriginalKey ? key : assembleGroupedKey(key, previousVersion);
                    if (!assembled.equals(previousAssembled)) {
                        // Remove old version for this msg from the grouped store - apparently it is now
                        // mapped to a different assembled key
                        lookupStore.delete(previousAssembled);
                    }
                }
                lookupStore.put(assembled, msg.now());
                mappingStore.put(key, msg);
            }

            context().forward(assembled, msg.now());
        }
    }

    private String assembleGroupedKey(String key, ReplicationMessage msg) {
        String extracted = keyExtract.apply(msg);
        if (extracted.indexOf('|') != -1) {
            throw new IllegalArgumentException("Can't prefix key. Already a grouped key: " + extracted + " grouping with: " + key);
        }
        if (key.indexOf('|') != -1) {
            throw new IllegalArgumentException("Can't prefix with key. Already a grouped key: " + key + " prepending with: " + extracted);
        }
        if (ignoreOriginalKey) {
            return extracted;
        }
        return extracted + "|" + key;
    }


}
