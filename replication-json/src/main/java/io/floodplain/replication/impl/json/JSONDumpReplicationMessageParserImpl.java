package io.floodplain.replication.impl.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static io.floodplain.immutable.api.ImmutableMessage.ValueType;

@Named("ndjson")
@ApplicationScoped
public class JSONDumpReplicationMessageParserImpl implements ReplicationMessageParser {

    private final static Logger logger = LoggerFactory.getLogger(JSONDumpReplicationMessageParserImpl.class);

    private final boolean includeNullValues = true;


//	public JSONReplicationMessageParserImpl() {
//		this(false);
//	}
//	
//	
//	public JSONReplicationMessageParserImpl() {
//	}

    public ReplicationMessage parseJson(Optional<String> source, ObjectNode on) {
        return ReplicationJSON.parseJSON(source, on);
    }

    protected boolean prettyPrint() {
        return false;
    }

    @Override
    public ReplicationMessage parseBytes(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return ReplicationJSON.parseReplicationMessage(data, Optional.empty(), ReplicationJSON.objectMapper);
        } catch (JsonProcessingException e) {
            return ReplicationFactory.createErrorReplicationMessage(e);
        } catch (Throwable e) {
            return ReplicationFactory.createErrorReplicationMessage(e);
        }
    }


    @Override
    public ReplicationMessage parseBytes(Optional<String> source, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return ReplicationJSON.parseReplicationMessage(data, source, ReplicationJSON.objectMapper);
        } catch (JsonProcessingException e) {
            return ReplicationFactory.createErrorReplicationMessage(e);
        } catch (Throwable e) {
            return ReplicationFactory.createErrorReplicationMessage(e);
        }
    }


    @Override
    public ReplicationMessage parseStream(InputStream data) {
        return parseStream(Optional.empty(), data);
    }

    @Override
    public ReplicationMessage parseStream(Optional<String> source, InputStream data) {
        JsonNode node;
        try {
            node = ReplicationJSON.objectMapper.readTree(data);
            return ReplicationJSON.parseJSON(source, (ObjectNode) node);
        } catch (JsonProcessingException e) {
            return ReplicationFactory.createErrorReplicationMessage(e);
        } catch (IOException e) {
            return ReplicationFactory.createErrorReplicationMessage(e);
        }
    }

    @Override
    public List<ReplicationMessage> parseMessageList(Optional<String> source, byte[] data) {
        try {
            JsonNode node = ReplicationJSON.objectMapper.readTree(data);
            return parseJSONNode(source, node);
        } catch (JsonProcessingException e) {
            List<ReplicationMessage> result = new LinkedList<>();
            result.add(ReplicationFactory.createErrorReplicationMessage(e));
            return Collections.unmodifiableList(result);
        } catch (Throwable e) {
            List<ReplicationMessage> result = new LinkedList<>();
            result.add(ReplicationFactory.createErrorReplicationMessage(e));
            return Collections.unmodifiableList(result);
        }
    }

    private List<ReplicationMessage> parseJSONNode(Optional<String> source, JsonNode node) {
        List<ReplicationMessage> result = new LinkedList<>();
        if (!(node instanceof ArrayNode)) {
            logger.warn("Node is not an array, so can't parse to list, will create list of one!");
            ObjectNode on = (ObjectNode) node;
            ReplicationMessage single = ReplicationJSON.parseJSON(source, on);
            result.add(single);
            return Collections.unmodifiableList(result);
        }
        ArrayNode elements = (ArrayNode) node;
        for (JsonNode jsonNode : elements) {
            ObjectNode on = (ObjectNode) jsonNode;
            result.add(ReplicationJSON.parseJSON(source, on));
        }
        return Collections.unmodifiableList(result);
    }


    @Override
    public byte[] serializeMessageList(List<ReplicationMessage> data) {
        ArrayNode list = ReplicationJSON.objectMapper.createArrayNode();
        data.stream().map(msg -> ReplicationJSON.toJSON(msg, includeNullValues)).forEach(e -> list.add(e));
        try {
            ObjectWriter w = ReplicationJSON.objectMapper.writer();
            return w.writeValueAsBytes(list);
        } catch (JsonProcessingException e) {
            logger.error("Error: ", e);
            // TODO, what is wise to return in this case?
            return null;
        }
    }

    @Override
    // TODO this change, too much copypaste
    public byte[] serialize(ReplicationMessage msg) {
        return ReplicationJSON.jsonSerializer(msg, includeNullValues, true);
    }

    @Override
    public String describe(ReplicationMessage msg) {
        return ReplicationJSON.jsonDescriber(msg, this);
    }

    @Override
    public List<ReplicationMessage> parseMessageList(Optional<String> source, InputStream data) {
        try {
            JsonNode node = ReplicationJSON.objectMapper.readTree(data);
            return parseJSONNode(source, node);
        } catch (JsonProcessingException e) {
            List<ReplicationMessage> result = new LinkedList<>();
            result.add(ReplicationFactory.createErrorReplicationMessage(e));
            return Collections.unmodifiableList(result);
        } catch (Throwable e) {
            List<ReplicationMessage> result = new LinkedList<>();
            result.add(ReplicationFactory.createErrorReplicationMessage(e));
            return Collections.unmodifiableList(result);
        }
    }

    @Override
    public List<ReplicationMessage> parseMessageList(byte[] data) {
        return parseMessageList(Optional.empty(), data);
    }
}
