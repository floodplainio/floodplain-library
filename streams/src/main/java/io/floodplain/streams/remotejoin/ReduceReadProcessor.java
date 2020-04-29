package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.factory.ImmutableFactory;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ReduceReadProcessor extends AbstractProcessor<String, ReplicationMessage> {

    private final String accumulatorStoreName;
    private final String inputStoreName;
    private final Function<ImmutableMessage,ImmutableMessage> initial;
    private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;
    private KeyValueStore<String, ImmutableMessage> accumulatorStore;
    private KeyValueStore<String, ReplicationMessage> inputStore;

    public ReduceReadProcessor(String inputStoreName, String accumulatorStoreName, Function<ImmutableMessage,ImmutableMessage> initial, Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor) {
        this.accumulatorStoreName = accumulatorStoreName;
        this.inputStoreName = inputStoreName;
        this.initial = initial;
        this.keyExtractor = keyExtractor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        accumulatorStore = (KeyValueStore<String, ImmutableMessage>) context.getStateStore(accumulatorStoreName);
        inputStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(inputStoreName);
        super.init(context);
    }

    @Override
    public void process(String key, final ReplicationMessage inputValue) {
        ReplicationMessage stored = inputStore.get(key);
        String extracted;
        if (stored == null) {
            // no stored value, so must be upsert.
            if (inputValue == null || inputValue.operation() == Operation.DELETE) {
                throw new RuntimeException("Issue: Deleting (?) a message that isn't there. Is this bad?");
            }
            extracted = keyExtractor.orElse((m, s) -> StoreStateProcessor.COMMONKEY)
                    .apply(inputValue.message(), inputValue.paramMessage().orElse(ImmutableFactory.empty()));
        } else {
            extracted = keyExtractor.orElse((m, s) -> StoreStateProcessor.COMMONKEY)
                    .apply(stored.message(), stored.paramMessage().orElse(ImmutableFactory.empty()));
        }
        ImmutableMessage storedAccumulator = this.accumulatorStore.get(extracted);
        ReplicationMessage value = inputValue;
        inputStore.put(key, inputValue);
        if (inputValue == null || inputValue.operation() == Operation.DELETE) {
            if (stored == null) {
                throw new RuntimeException("Issue: Deleting a message that isn't there. Is this bad?");
            }
            // delete
            ImmutableMessage param = storedAccumulator == null ? initial.apply(stored.message()) : storedAccumulator;
            value = stored.withOperation(Operation.DELETE).withParamMessage(param);
            inputStore.delete(key);
        } else {
            if(storedAccumulator==null) {
                storedAccumulator = initial.apply(inputValue.message());
            }
            value = value.withParamMessage(storedAccumulator);
            if (stored != null) {
                // already present, propagate old value first as delete
//                context().forward(key, stored.withOperation(Operation.DELETE).withParamMessage(storedAccumulator != null ? storedAccumulator : initial.apply(inputValue.message())));
                forwardMessage(key,stored.withOperation(Operation.DELETE).withParamMessage(storedAccumulator != null ? storedAccumulator : initial.apply(inputValue.message())));
                storedAccumulator = this.accumulatorStore.get(extracted);
            }
            value = value.withParamMessage(storedAccumulator);
        }
        forwardMessage(key,value);

    }

    private void forwardMessage(String key, ReplicationMessage msg) {
        context().forward(key, msg);
    }
}
