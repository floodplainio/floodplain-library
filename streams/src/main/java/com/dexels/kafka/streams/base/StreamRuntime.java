package com.dexels.kafka.streams.base;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.transformer.custom.*;
import com.dexels.replication.transformer.api.MessageTransformer;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.apache.kafka.clients.admin.AdminClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;

//@Component(name="kafka.stream.runtime", service = {StreamRuntime.class}, immediate=true)
public class StreamRuntime {

    @ConfigProperty(name = "stream.generation")
    Optional<String> configuredGeneration;

    private static final Logger logger = LoggerFactory.getLogger(StreamRuntime.class);
    private final Map<String, StreamInstance> streams = new HashMap<>();

    private StreamConfiguration configuration;
    private final List<String> instanceFilter;
    private final Set<StreamInstance> startedInstances = new HashSet<>();
    Subject<Runnable> updateQueue = PublishSubject.<Runnable>create().toSerialized();
    private Disposable updateQueueSubscription;

    private final Map<String, MessageTransformer> transformerRegistry = new HashMap<>();

    private AdminClient adminClient;
    private final String deployment;
    private final File path;
    private final File outputFolder;

    public StreamRuntime(String deployment, File path, File outputFolder) {
        this.deployment = deployment;
        this.path = path;
        this.outputFolder = outputFolder;
        String filter = System.getenv("FILTER_INSTANCES");
        if (filter == null || "".equals(filter)) {
            this.instanceFilter = Collections.emptyList();
        } else {
            this.instanceFilter = Arrays.asList(filter.split(","));
        }
        transformerRegistry.put("formatgender", new FormatGenderTransformer());
        transformerRegistry.put("createlist", new CreateListTransformer());
        transformerRegistry.put("splitToList", new SplitToListTransformer());
        transformerRegistry.put("copyfield", new CopyFieldTransformer());
        transformerRegistry.put("publicid", new CreatePublicIdTransformer());
        transformerRegistry.put("formatcommunication", new CommunicationTransformer());
        transformerRegistry.put("stringtodate", new StringToDateTransformer());
        transformerRegistry.put("formatzipcode", new FormatZipCodeTransformer());
        transformerRegistry.put("teamname", new TeamName());
        transformerRegistry.put("mergedatetime", new MergeDateTimeTransformer());
        transformerRegistry.put("createcoordinate", new CreateCoordinateTransformer());
        transformerRegistry.put("fieldtoupper", (params, msg) -> msg.with(params.get("field"), ((String) (msg.columnValue(params.get("field")))).toString().toUpperCase(), ImmutableMessage.ValueType.STRING));
        transformerRegistry.put("fieldtolower", (params, msg) -> msg.with(params.get("field"), ((String) (msg.columnValue(params.get("field")))).toString().toLowerCase(), ImmutableMessage.ValueType.STRING));
        transformerRegistry.put("emailtolower", (params, msg) -> {
            if ("EMAIL".equals(msg.columnValue("typeofcommunication"))) {
                return msg.with("communicationdata", ((String) msg.columnValue("communicationdata")).toLowerCase(), ImmutableMessage.ValueType.STRING);
            } else {
                return msg;
            }
        });
        this.updateQueueSubscription = updateQueue.observeOn(Schedulers.from(Executors.newSingleThreadExecutor()))
                .subscribe(r -> r.run());
    }

    public Optional<String> configuredGeneration() {
        return configuredGeneration;
    }

    private synchronized void startInstances() {
        streams.entrySet().forEach(e -> {
            StreamInstance in = e.getValue();
            if (startedInstances.contains(in)) {
                logger.warn("Instance: {} has started already, ignoring restart", e.getKey());
            } else {
                if (in == null) {
                    logger.warn("Whoa! StreamInstance not found: {}, ignoring", e.getKey());
                } else {
                    startedInstances.add(in);
                    logger.info("Starting instance: {} from thread: {}", e.getKey(), Thread.currentThread().getName());
                    new Thread(() -> in.start()).start();
                }
            }

        });

    }

    public static String nameFromFileName(String fullpath) {
        String path = fullpath.split("\\.")[0];
        String[] pathelements = path.split("/");
        String[] pathparts = pathelements[pathelements.length - 1].split("-");
        return pathparts.length > 1 ? pathparts[0] : pathelements[pathelements.length - 1];
    }

    private String friendlyName(String name) {
        if (name.endsWith(".xml")) {
            name = name.substring(0, name.length() - 4);
        }
        return name.replaceAll("/", "-");
    }

    @PreDestroy
    public void deactivate() {
        logger.info("Starting deactivate of Kafka Streams");
        if (this.updateQueueSubscription != null) {
            this.updateQueueSubscription.dispose();
        }

        Observable.fromIterable(streams.entrySet())
                .observeOn(Schedulers.newThread())
                .subscribe(si -> {
                    try {
                        si.getValue().shutdown();
                    } catch (Throwable e) {
                        logger.error("Error shutting down instance: " + si.getValue(), e);
                    }

                });
        streams.clear();
        this.startedInstances.clear();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            logger.error("Error: ", e);
        }
        logger.info("Deactivate of Kafka Streams complete");
    }


    private File getOutputFolder() {
        return new File(this.outputFolder, "storage");
    }

    public Map<String, StreamInstance> getStreams() {
        return streams;

    }
}
