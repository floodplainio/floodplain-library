package io.floodplain.streams.base;

import io.floodplain.replication.api.ReplicationMessage;
import io.reactivex.functions.Function3;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Filters {


    private final static Logger logger = LoggerFactory.getLogger(Filters.class);


    private static Map<String, Function3<String, List<String>, ReplicationMessage, Boolean>> predicates = new HashMap<>();

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
        Filters.registerPredicate("validZip", Filters::isValidZipCode);
    }

    private static Function3<String, List<String>, ReplicationMessage, Boolean> equalToAnyIn() {
        return (id, params, message) -> {
            if (params.size() < 2) {
                throw new Exception("Need some more arguments, at least two. I got: " + params);
            }
            String columnValue = (String) message.columnValue(params.get(0));
            return params.stream().skip(1).anyMatch(item -> item.equals(columnValue));
        };
    }

    public static void registerPredicate(String name, Function3<String, List<String>, ReplicationMessage, Boolean> predicate) {
        predicates.put(name, predicate);
    }

    public static Optional<Predicate<String, ReplicationMessage>> getFilter(Optional<String> filter) {
        if (!filter.isPresent()) {
            return Optional.empty();
        }
        String filterString = filter.get();
        logger.info("Parsing filter: " + filterString);
        if (filterString.startsWith("!")) {
            final Predicate<String, ReplicationMessage> predicate = getCoreFilter(filterString.substring(1));
            return Optional.of((key, value) -> !predicate.test(key, value));
        }
        return Optional.of(getCoreFilter(filterString));

    }

    public static Predicate<String, ReplicationMessage> getCoreFilter(String filterString) {
        logger.info("Core Parsing filter: " + filterString);

        String[] splitForOr = filterString.split("\\|");
        List<Predicate<String, ReplicationMessage>> allPredicates = new ArrayList<>();
        for (String p : splitForOr) {
            String[] parts = p.split(",");
            List<Predicate<String, ReplicationMessage>> predicateList = new ArrayList<>();
            for (String part : parts) {
                Predicate<String, ReplicationMessage> parsed = parseReplicationFilter(part);
                predicateList.add(parsed);
            }
            // use and
            allPredicates.add((key, value) -> {
                for (Predicate<String, ReplicationMessage> pred : predicateList) {
                    boolean res;
                    try {
                        res = pred.test(key, value);
                    } catch (Throwable t) {
                        logger.error("Exception on checking key: {}, value: {}", key, value, t);
                        res = true;
                    }
                    if (!res) {
                        return false;
                    }
                }
                return true;
            });
        }

        return (key, value) -> {
            for (Predicate<String, ReplicationMessage> pred : allPredicates) {
                boolean res = pred.test(key, value);
                if (res) {
                    return true;
                }
            }
            return false;
        };
    }

    private static Predicate<String, ReplicationMessage> parseReplicationFilter(String filterDefinition) {
        String[] parts = filterDefinition.split(":");
        String command = parts[0];
        List<String> params;
        if (parts.length > 1) {
            String rest = filterDefinition.substring(command.length() + 1);
            params = Arrays.asList(rest.split(":"));
        } else {
            params = Collections.emptyList();
        }
        Function3<String, List<String>, ReplicationMessage, Boolean> cmd = predicates.get(command);
        if (cmd == null) {
            logger.error("Unable to locate predicate with name: " + command);
            logger.info("Available predicates: " + predicates.keySet());
        }
        return (key, msg) -> {
            try {
                return cmd.apply(key, params, msg);
            } catch (Exception e) {
                throw new RuntimeException("Error processing message filter", e);
            }
        };
    }


    private static boolean isValidZipCode(String id, List<String> params, ReplicationMessage msg) {
        String zipColumn = params.get(0);
        Object val = msg.columnValue(zipColumn);
        if (val == null) {
            return false;
        }
        if (!(val instanceof String)) {
            return false;
        }
        String zipString = (String) val;
        if (zipString.length() == 6) {
            String numbers = zipString.substring(0, 4);
            return numbers.chars().allMatch(Character::isDigit);
        } else if (zipString.length() == 7) {
            String numbers = zipString.substring(0, 4);
            return numbers.chars().allMatch(Character::isDigit);
        } else {
            return false;
        }
    }


}
