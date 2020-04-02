package com.dexels.kafka.webapi;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.api.sinkdefinition.SinkTemplateDefinition;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.osgi.service.component.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

@Component(configurationPolicy = ConfigurationPolicy.IGNORE, name = "dexels.mongo.servlet", property = {"servlet-name=dexels.mongo.servlet", "alias=/mongogeneration", "asyncSupported.Boolean=true"}, immediate = true, service = {Servlet.class})
public class MongoServlet extends HttpServlet {

    private static final long serialVersionUID = 9041611204663809500L;
    private final Map<String, SinkTemplateDefinition> templates = new HashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(MongoServlet.class);


    @Activate
    public void activate() {
        //noop
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String pathInfo = req.getPathInfo();
        if (pathInfo == null) {
            resp.getWriter().write("Usage /mongogeneration/[tenant]/[deployment]/[generation]/[name]");
            return;
        }
        String[] parts = pathInfo.split("/");
        if (parts.length < 5) {
            resp.getWriter().write("Usage /mongogeneration/[tenant]/[deployment]/[generation]/[name]");
            return;
        }
        String tenant = parts[1];
        String deployment = parts[2];
        String generation = parts[3];

        // TODO remove null ref?
        TopologyContext topologyContext = new TopologyContext(Optional.of(tenant), deployment, null, generation);
        String name = parts[4];
        SinkTemplateDefinition std = templates.get(name);
        if (std == null) {
            resp.getWriter().write("Missing definition: " + name);
            return;
        }
        String host = (String) std.getSettings().get("host");
        String port = (String) std.getSettings().get("port");
        String database = (String) std.getSettings().get("database");
        String siblingGeneration = req.getParameter("sibling");
        TopologyContext siblingContext = new TopologyContext(Optional.of(tenant), deployment, null, generation);

        String resolvedDatabaseName = CoreOperators.topicName(database, topologyContext);
        String resolvedSiblingDatabaseName = CoreOperators.topicName(database, siblingContext);
        MongoDatabase nb = getMongoInstance(host, port, resolvedDatabaseName);
        MongoDatabase siblingDatabase = getMongoInstance(host, port, resolvedSiblingDatabaseName);
        List<String> p = generationStats(nb, siblingDatabase);
        for (String line : p) {
            resp.getWriter().write(line + "\n");
        }
    }

    public MongoDatabase getMongoInstance(String host, String port, String database) {
        String connectionString = "mongodb://" + host + ":" + port + "/" + database;
        return MongoClients.create(connectionString).getDatabase(database);
    }

    public List<String> generationStats(MongoDatabase database, MongoDatabase sibling) {
        List<String> result = new ArrayList<>();
        int collectionCount = 0;
        for (String collectionName : database.listCollectionNames()) {
            Document originalStats = database.runCommand(new Document("collStats", collectionName));
            MongoCollection<Document> siblingCollection = sibling.getCollection(collectionName);
            if (siblingCollection == null) {
                result.add("Missing collection in sibling: " + collectionName);
                continue;
            } else {
                result.add("collection found: " + collectionName);
            }
            try {
                Document siblingStats = sibling.runCommand(new Document("collStats", collectionName));
                long count = originalStats.getInteger("count");
                long siblingCount = siblingStats.getInteger("count");
                if (count != siblingCount) {
                    result.add("Count difference in collection: " + collectionName + " (" + count + "/" + siblingCount + ")");
                } else {
                    Double size = extractKey(originalStats, "size");
                    Double siblingSize = extractKey(siblingStats, "size");
                    if (Math.abs(size - siblingSize) < 1) {
                    } else {
                        result.add("Size difference in collection: " + collectionName + " (" + size + "/" + siblingSize + ")");
                    }
                }
                Document diff = diff(collectionName, originalStats, siblingStats);
            } catch (Exception e) {
                logger.error("Error: ", e);
            }
            collectionCount++;
        }
        result.add("Total collections: " + collectionCount);
        return result;
    }

    // stupid hack appears necessary because the type seems to fluctuate
    private Double extractKey(Document d, String key) {
        Object p = d.get(key);
        if (p == null) {
            return null;
        } else if (p instanceof Double) {
            return (Double) p;
        } else if (p instanceof Integer) {
            return new Double((Integer) p);
        }
        logger.info("Weird type: {} in doc: {} p: {}", key, d.toJson(), p.getClass());
        return 0d;
    }

    // TODO
    public List<String> diffCollection(MongoDatabase database, MongoDatabase sibling, String collectionName) {
        return Collections.emptyList();
    }

    private Document diff(String collectionName, Document originalStats, Document siblingStats) {
        Document res = new Document();
        res.append("originalCount", originalStats.get("count"));
        res.append("siblingCount", siblingStats.get("count"));
        res.append("originalSize", originalStats.get("size"));
        res.append("siblingSize", siblingStats.get("size"));
        return res;
    }


    @Reference(cardinality = ReferenceCardinality.MULTIPLE, unbind = "removeSinkTemplate", policy = ReferencePolicy.DYNAMIC, target = "(type=mongodb.sink)")
    public void addSinkTemplate(SinkTemplateDefinition def) {
        templates.put(def.getName(), def);
    }

    public void removeSinkTemplate(SinkTemplateDefinition def) {
        templates.remove(def.getName());
    }

}
