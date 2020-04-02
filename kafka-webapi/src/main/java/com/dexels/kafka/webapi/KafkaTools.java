package com.dexels.kafka.webapi;

import com.dexels.pubsub.rx2.api.PersistentPublisher;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import java.io.Writer;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaTools {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTools.class);

    private KafkaTools() {
        // -- no instances
    }

    private static Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    private static Predicate<String> includeGroup = input -> {
        if (input.startsWith("NavajoLink") || input.startsWith("console-consumer") || input.startsWith("AAAResetter")) {
            return false;
        }
        if (input.startsWith("navajo-monitor")) {
            return false;
        }
        if (input.startsWith("KafkaManager")) {
            return false;
        }
        final Matcher matcher = pattern.matcher(input);
        return !(matcher.matches());
    };


    public static Single<TopicStructure> getTopics(PersistentPublisher publisher) {
        return publisher.streamTopics()
                .reduce(new TopicStructure(), (stru, e) -> stru.consumeTopic(e));
    }


    public static Single<GroupStructure> getGroupStructure(PersistentPublisher publisher) {
        return publisher.listConsumerGroups()
                .filter(includeGroup)
                .reduce(new GroupStructure(), (struct, g) -> struct.consumeGroup(g));
    }


    public static void deleteDryRun(PersistentPublisher publisher, String tenant, String deployment, String generation, Writer writer) throws ServletException {
        getGenerationGroups(publisher, tenant, deployment, generation)
                .buffer(10)
                .blockingForEach(e -> writer.write("Would delete: " + e + "\n"));
    }

    public static Completable deleteGroups(PersistentPublisher publisher, String tenant, String deployment, String generation, Writer writer) throws ServletException {
        return getGenerationGroups(publisher, tenant, deployment, generation)
                .doOnNext(e -> writer.write("Deleting group: " + e + "\n"))
                .buffer(10)
                .flatMapCompletable(e -> publisher.deleteGroups(e));
    }

    private static Observable<String> getGenerationGroups(PersistentPublisher publisher, String tenant, String deployment, String generation) throws ServletException {
        if (tenant == null) {
            throw new ServletException("No tenant supplied!");
        }
        if (deployment == null) {
            throw new ServletException("No deployment supplied!");
        }
        if (generation == null) {
            throw new ServletException("No generation supplied!");
        }
        Set<String> groups = KafkaTools.getGroupStructure(publisher)
                .blockingGet()
                .getTenant(tenant)
                .getDeployment(deployment)
                .getGeneration(generation)
                .getGroups();
        return Observable.fromIterable(groups);

    }

    public static Completable deleteGenerationTopics(PersistentPublisher publisher, Writer out, String tenant, String deployment, String generation) {
        if (tenant == null) {
            return Completable.error(new ServletException("No tenant supplied!"));
        }
        if (deployment == null) {
            return Completable.error(new ServletException("No deployment supplied!"));
        }
        if (generation == null) {
            return Completable.error(new ServletException("No generation supplied!"));
        }
        TopicStructure struct = KafkaTools.getTopics(publisher).blockingGet();

        com.dexels.kafka.webapi.TopicStructure.Generation g = struct.getTenant(tenant).getDeployment(deployment).getGeneration(generation);
        if (g.topicCount() == 0) {
            logger.warn("No matching topics!");
        }
        logger.info("About to delete generation: {} on deployment: {} and tenant: {}", generation, deployment, tenant);

        return Flowable.fromIterable(g.getTopics())
                .doOnNext(e -> {
                    out.write("Deleting topic: " + e + "\n");
                    out.flush();
                })
                .buffer(20)
                .flatMapCompletable(e -> publisher.deleteCompletable(e))
                .doOnComplete(() -> logger.info("done!"));
    }


}
