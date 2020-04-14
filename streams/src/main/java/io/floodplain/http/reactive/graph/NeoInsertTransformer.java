package io.floodplain.http.reactive.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.floodplain.http.reactive.http.HttpInsertTransformer;
import io.floodplain.http.reactive.http.ReactiveReply;
import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.immutable.json.ImmutableJSON;
import io.floodplain.replication.api.ReplicationMessage;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.apache.commons.text.StringSubstitutor;
import org.bson.internal.Base64;
import org.eclipse.jetty.http.HttpMethod;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NeoInsertTransformer implements FlowableTransformer<List<ReplicationMessage>, Flowable<byte[]>> {

    private final String labels;
    private final Type type;


    private final static Logger logger = LoggerFactory.getLogger(NeoInsertTransformer.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private enum Type {
        RELATION,
        NODE
    }

    public static FlowableTransformer<List<ReplicationMessage>, Flowable<byte[]>> neoNodeTransformer(String labels) {
        return new NeoInsertTransformer(labels, Type.NODE);
    }

    public static FlowableTransformer<List<ReplicationMessage>, Flowable<byte[]>> neoRelationshipTransformer(String labels) {
        return new NeoInsertTransformer(labels, Type.RELATION);
    }


    public static FlowableTransformer<Flowable<byte[]>, ReactiveReply> neoInserter(String url, String username, String password) {
        String baseauth = "Basic " + Base64.encode((username + ":" + password).getBytes());
        return HttpInsertTransformer.httpInsert(url, req -> req.method(HttpMethod.POST)
                        .header("Authorization", baseauth)
                , "application/json; charset=UTF-8", 1, 1, true);
    }

    private NeoInsertTransformer(String labels, Type type) {
        this.labels = labels;
        this.type = type;
    }

    private static Flowable<byte[]> statements(List<ReplicationMessage> messages, String labels) throws IOException {
        ArrayNode node = objectMapper.createArrayNode();
        for (ReplicationMessage msg : messages) {
            node.add(insertStatement(msg, labels));
        }
        ObjectNode root = objectMapper.createObjectNode();
        root.set("statements", node);
        System.err.println("request:\n" + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));

        return serializeJson(root);
    }

    private static Flowable<byte[]> serializeJson(ObjectNode node) {
        try {
            return Flowable.just(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(node));
        } catch (JsonProcessingException e) {
            return Flowable.error(e);
        }
    }


    private static ObjectNode insertStatement(ReplicationMessage message, String labels) throws IOException {
        String key = message.combinedKey();
        String statement = "CREATE (n:${label} $props) RETURN n";
        final ObjectNode json = ImmutableJSON.json(message.message().with("key", key, ImmutableMessage.ValueType.STRING));

        StringSubstitutor sub = new StringSubstitutor(res -> {
            switch (res) {
                case "label":
                    return labels;
                default:
                    return "aap";
            }
        });
        ObjectNode statementNode = objectMapper.createObjectNode();
        final String replacedStatement = sub.replace(statement);
        statementNode.put("statement", replacedStatement);
        ObjectNode parametersNode = objectMapper.createObjectNode();
        statementNode.set("parameters", parametersNode);
        parametersNode.put("id", key);
        parametersNode.set("props", json);
        return statementNode;
    }

    private Flowable<byte[]> relationStatements(List<ReplicationMessage> messages, String sourcelabel, String destinationlabel, String sourcekey, String destinationkey, String relationlabel) throws IOException {
        ArrayNode node = objectMapper.createArrayNode();
        ObjectNode root;
        try {
            for (ReplicationMessage msg : messages) {
                node.add(insertRelation(msg, sourcelabel, destinationlabel, sourcekey, destinationkey, relationlabel));
            }
            root = objectMapper.createObjectNode();
            root.set("statements", node);
            return serializeJson(root);
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
        return null;
    }


    private ObjectNode insertRelation(ReplicationMessage message, String sourcelabel, String destinationlabel, String sourcekey, String destinationkey, String relationlabel) throws IOException {
        String key = message.combinedKey();
        Map<String, String> inlineparams = new HashMap<>();
        inlineparams.put("sourcelabel", sourcelabel);
        inlineparams.put("destinationlabel", destinationlabel);
        inlineparams.put("sourcekey", sourcekey);
        inlineparams.put("destinationkey", destinationkey);
        inlineparams.put("relationlabel", relationlabel);
        StringSubstitutor sub = new StringSubstitutor(inlineparams);

        String statement = "MATCH (f:${sourcelabel}),(a:${destinationlabel}) WHERE f.${sourcekey} = $sourcekey AND a.${destinationkey} = $destinationkey CREATE (f)-[r:${relationlabel} {props} ]->(a) RETURN r";
        final ObjectNode json = ImmutableJSON.json(message.message().with("key", key, ImmutableMessage.ValueType.STRING));

        ObjectNode statementNode = objectMapper.createObjectNode();
        final String replacedStatement = sub.replace(statement);
        statementNode.put("statement", replacedStatement);
        ObjectNode parametersNode = objectMapper.createObjectNode();
        statementNode.set("parameters", parametersNode);
        putValue(parametersNode, message, sourcekey, "sourcekey");
        putValue(parametersNode, message, destinationkey, "destinationkey");
//		String params = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parametersNode);
        parametersNode.set("props", json);
        System.err.println("Statement:\n" + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(statementNode));
        return statementNode;
    }

    private void putValue(ObjectNode node, ReplicationMessage message, String name, String paramName) {
        ImmutableMessage.ValueType tp = message.columnType(name);
        switch (tp) {
            case STRING:
                node.put(paramName, (String) message.columnValue(name));
                return;
            case INTEGER:
                node.put(paramName, (Integer) message.columnValue(name));
                return;
            case LONG:
                node.put(paramName, (Long) message.columnValue(name));
                return;
            default:
                return; // TODO finish types
        }
    }


    @Override
    public Publisher<Flowable<byte[]>> apply(Flowable<List<ReplicationMessage>> in) {
        switch (this.type) {
            case RELATION:
                return in.map(e -> relationStatements(e, "ACTOR", "FILM", "actor_id", "film_id", "PLAYS_IN"));
            case NODE:
            default:
                return in.map(e -> statements(e, labels));
        }
    }


}
