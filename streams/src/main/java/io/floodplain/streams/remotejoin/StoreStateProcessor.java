package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class StoreStateProcessor extends AbstractProcessor<String, ReplicationMessage> {


    private final static Logger logger = LoggerFactory.getLogger(StoreStateProcessor.class);
    private final String name;
    private final String lookupStoreName;
    private final Function<ImmutableMessage,ImmutableMessage> initial;
    private KeyValueStore<String, ImmutableMessage> lookupStore;
    private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;
    public static final String COMMONKEY = "singlerestore";

    public StoreStateProcessor(String name, String lookupStoreName, Function<ImmutableMessage,ImmutableMessage> initial, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {
        this.name = name;
        this.lookupStoreName = lookupStoreName;
        this.initial = initial;
        this.keyExtractor = keyExtractor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.lookupStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(lookupStoreName);
        super.init(context);
    }

    @Override
    public void process(String key, ReplicationMessage inputValue) {

        String extracted = keyExtractor.orElse((msg, state) -> COMMONKEY).apply(inputValue.message(), inputValue.paramMessage().orElse(ImmutableFactory.empty())); //  keyExtractor.map(e->e.apply(Optional.of(inputValue.message()),inputValue.paramMessage())).map(e->(String)e.value);
        ImmutableMessage paramMessage = inputValue.message(); //.get();
        lookupStore.put(extracted, paramMessage);
        super.context().forward(extracted, inputValue.withOperation(Operation.UPDATE));
    }

}
