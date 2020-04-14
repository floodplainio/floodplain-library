package io.floodplain.reactive.topology;

import io.floodplain.reactive.source.topology.api.TopologyPipeComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class ReactivePipe {
    public final TopologyPipeComponent source;
    public final List<TopologyPipeComponent> transformers;
    Optional<String> binaryMime = Optional.empty();

    private static final Logger logger = LoggerFactory.getLogger(ReactivePipe.class);

    public ReactivePipe(TopologyPipeComponent source, List<TopologyPipeComponent> transformers) {
        this.source = source;
        this.transformers = transformers;
//		typecheck();
    }

}
