package io.floodplain.reactive.source.topology;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import io.floodplain.streams.api.TopologyContext;
import io.floodplain.streams.remotejoin.ReplicationTopologyParser;
import io.floodplain.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.function.BiFunction;

public class ScanTransformer implements TopologyPipeComponent {

    private final ImmutableMessage initial;
    private final List<TopologyPipeComponent> onAdd;
    private final List<TopologyPipeComponent> onRemove;
    private boolean materialize;
    private final Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor;


    //	public ScanTransformer(ImmutableMessage initial, List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove) {
//		this(null,initial,onAdd,onRemove);
//	}
    public ScanTransformer(BiFunction<ImmutableMessage, ImmutableMessage, String> keyExtractor, ImmutableMessage initial, List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove) {
        this.keyExtractor = Optional.ofNullable(keyExtractor);
        this.initial = initial;
        this.onAdd = onAdd;
        this.onRemove = onRemove;
    }

    @Override
    public void addToTopology(Stack<String> transformerNames, int currentPipeId, Topology topology,
                              TopologyContext topologyContext, TopologyConstructor topologyConstructor) {
//		Function<ReplicationMessage,String> keyXtr = msg->{
//			return this.keyExtractor.apply(msg.message(),msg.paramMessage().orElse(ImmutableFactory.empty()));
//		};
//		Optional<ContextExpression> keyExtractor = Optional.ofNullable(parameters.named.get("key"));
        String reducerName = ReplicationTopologyParser.addReducer(topology, topologyContext, topologyConstructor, topologyContext.instance, transformerNames, currentPipeId, onAdd, onRemove, initial, materialize, keyExtractor);
        transformerNames.push(reducerName);
    }

    @Override
    public boolean materializeParent() {
        return false;
    }

    @Override
    public void setMaterialize() {
        this.materialize = true;
    }

    @Override
    public boolean materialize() {
        return this.materialize;
    }


}
