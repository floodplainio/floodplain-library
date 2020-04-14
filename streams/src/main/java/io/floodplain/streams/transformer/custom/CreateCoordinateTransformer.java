package io.floodplain.streams.transformer.custom;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.api.customtypes.CoordinateType;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.transformer.api.MessageTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// TODO this name is weird
public class CreateCoordinateTransformer implements MessageTransformer {
    private final static Logger logger = LoggerFactory.getLogger(CreateCoordinateTransformer.class);

    @Override
    public ReplicationMessage apply(Map<String, String> params, ReplicationMessage msg) {

        String from = params.get("from");
        String to = params.get("to");

        String[] st = from.split(",");
        Object val1 = msg.columnValue(st[0]);
        Object val2 = msg.columnValue(st[1]);

        if (val1 == null || val2 == null) {
            return msg;
        }

        try {
            CoordinateType coor = new CoordinateType(val1, val2);
            return msg.with(to, coor, ImmutableMessage.ValueType.COORDINATE);
        } catch (Throwable e) {
            logger.warn("Error in transformer - skipping. Val1: {} val2: {}", val1, val2, e);
        }
        return msg;
    }

}
