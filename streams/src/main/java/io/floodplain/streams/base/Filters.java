package io.floodplain.streams.base;

import io.floodplain.replication.api.ReplicationMessage;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Filters {


    private final static Logger logger = LoggerFactory.getLogger(Filters.class);

    public static interface TriFunction {
        public boolean apply(String id, List<String> params, ReplicationMessage message);
    }

    private static Map<String, TriFunction> predicates = new HashMap<>();

    static {
        Filters.registerPredicate("clublogo", (id, params, message) -> "CLUBLOGO".equals(message.columnValue("objecttype")) && message.columnValue("data") != null);
        Filters.registerPredicate("photo", (id, params, message) -> "PHOTO".equals(message.columnValue("objecttype")) && message.columnValue("data") != null);
        Filters.registerPredicate("facility", (id, params, message) -> "FACILITY".equals(message.columnValue("facilitytype")));
        Filters.registerPredicate("subfacility", (id, params, message) -> !"FACILITY".equals(message.columnValue("facilitytype")));

        Filters.registerPredicate("subfacility_facility", (id, params, message) -> "FACILITY".equals(message.columnValue("subfacilityid")));
        Filters.registerPredicate("subfacility_not_facility", (id, params, message) -> !"FACILITY".equals(message.columnValue("subfacilityid")));
        Filters.registerPredicate("valid_calendar_activityid", (id, params, message) -> {
            if (message.columnValue("activityid") == null) {
                logger.warn("Null activityid! key: {}. Message: {}", message.queueKey(), message);
                return false;
            }
            return ((Integer) message.columnValue("activityid")) >= 20;
        });
        Filters.registerPredicate("notnull", (id, params, message) -> (message.columnValue(params.get(0))) != null);
        Filters.registerPredicate("null", (id, params, message) -> (message.columnValue(params.get(0))) == null);
        Filters.registerPredicate("greaterthan", (id, params, message) -> ((Integer) message.columnValue(params.get(0))) > Integer.parseInt(params.get(1)));
        Filters.registerPredicate("lessthan", (id, params, message) -> ((Integer) message.columnValue(params.get(0))) < Integer.parseInt(params.get(1)));
        Filters.registerPredicate("equalToString", (id, params, message) -> params.get(1).equals(message.columnValue(params.get(0))));
        Filters.registerPredicate("equalToAnyIn", equalToAnyIn());
        Filters.registerPredicate("equalToNoneIn", (id, params, message) -> !equalToAnyIn().apply(id, params, message));
        Filters.registerPredicate("equalToInt", (id, params, message) -> new Integer(Integer.parseInt(params.get(1))).equals(message.columnValue(params.get(0))));
        Filters.registerPredicate("notEqualToString", (id, params, message) -> !params.get(1).equals(message.columnValue(params.get(0))));
    }

    private static TriFunction equalToAnyIn() {
        return (id, params, message) -> {
            if (params.size() < 2) {
                throw new RuntimeException("Need some more arguments, at least two. I got: " + params);
            }
            String columnValue = (String) message.columnValue(params.get(0));
            return params.stream().skip(1).anyMatch(item -> item.equals(columnValue));
        };
    }

    public static void registerPredicate(String name,TriFunction predicate) {
        predicates.put(name, predicate);
    }
}
