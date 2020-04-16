package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CacheProcessor extends AbstractProcessor<String, ReplicationMessage> {
    private static final String CACHED_AT_KEY = "_cachedAt";
    private static final Integer DEFAULT_CACHE_TIME = 10;

    private static final Logger logger = LoggerFactory.getLogger(CacheProcessor.class);


    private final Map<String, CacheEntry> cache;
    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private ProcessorContext context;

    private final int cacheTimeMs;
    private String cacheProcName;
    private Object sync = new Object();

    private boolean memoryCache = false;
    private boolean clearPersistentCache = false;

    private int maxSize;

    public CacheProcessor(String cacheProcName, Optional<String> cacheTimeS, Optional<String> maxSizeS) {

        this.cacheProcName = cacheProcName;
        Integer cacheTime = getCacheTime(cacheTimeS);
        this.cache = new ConcurrentHashMap<>();
        if (cacheTime < 10) {
            logger.info("Using memory caching for {}", cacheProcName);
            // Use memory cache
            this.memoryCache = true;
            this.maxSize = 10000;
        }
        logger.info("Using a cache time of {} seconds for {}", cacheTime, cacheProcName);
        this.cacheTimeMs = (cacheTime * 1000);
    }

    /*
     * We can either pass no cache time, in which case we will return DEFAULT_CACHE_TIME
     * If we do have a cacheTime set, check if its a number or not. If it's not a number
     * then assume we want to use an environment variable. Try parsing it, and if all fails,
     * return DEFAULT_CACHE_TIME.
     */
    private Integer getCacheTime(Optional<String> cacheTime) {
        String cacheTimeS = null;
        if (cacheTime.isPresent()) {
            cacheTimeS = (String) cacheTime.get();
        } else {
            return DEFAULT_CACHE_TIME;
        }

        // Try parsing as integer. If this fails, it might be a environment var. if all fails, return default. 
        Integer cacheTimeI;
        try {
            cacheTimeI = Integer.parseInt(cacheTimeS);
        } catch (NumberFormatException e) {
            try { // Isn't this weird?
                cacheTimeI = Integer.parseInt(System.getenv(cacheTimeS));
            } catch (NumberFormatException e2) {
                logger.warn("Unable to parse cache time {}, using default", cacheTimeS);
                cacheTimeI = DEFAULT_CACHE_TIME;
            }
        }
        return cacheTimeI;

    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.context = context;
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(cacheProcName);

        int runInterval = Math.max((cacheTimeMs / 10), 1000);

        this.context.schedule(runInterval, PunctuationType.WALL_CLOCK_TIME, this::checkCache);
        logger.info("Created persistentCache for {} with check interval of {}ms", this.cacheProcName, runInterval);

        if (memoryCache) {
            clearPersistentCache = true;
        }
    }


    @Override
    public void process(String key, ReplicationMessage message) {
        synchronized (sync) {
            if (message == null || message.operation() == Operation.DELETE) {
                cache.remove(key);
                lookupStore.delete(key);
                context.forward(key, message);
            } else if (memoryCache) {
                if (cache.size() > maxSize) {
                    logger.warn("Reached max cache size!");
                    context.forward(key, message);
                } else {
                    cache.put(key, new CacheEntry(message));
                }
            } else {
                lookupStore.put(key, message.with(CACHED_AT_KEY, System.currentTimeMillis(), ImmutableMessage.ValueType.LONG));
            }
        }
    }


    @Override
    public void close() {
        synchronized (sync) {
            for (String key : cache.keySet()) {
                CacheEntry entry = cache.get(key);
                context.forward(key, entry.getEntry());
                cache.remove(key);
            }
        }
        super.close();
    }

    public void checkCache(long ms) {
        if (clearPersistentCache) {
            clearPersistentCache();
        }

        long started = System.currentTimeMillis();
        int entries = 0;
        int expiredEntries = 0;
        if (memoryCache) {
            Set<String> toForward = new HashSet<>();
            for (String key : cache.keySet()) {
                CacheEntry entry = cache.get(key);
                if (entry.isExpired()) {
                    toForward.add(key);
                }
            }
            synchronized (sync) {
                for (String key : toForward) {
                    if (cache.get(key) == null) continue; // message is deleted
                    context.forward(key, cache.get(key).getEntry());
                    cache.remove(key);
                }
            }
        } else {
            Set<String> possibleExpired = new HashSet<>();
            try (KeyValueIterator<String, ReplicationMessage> it = lookupStore.all()) {
                while (it.hasNext()) {
                    KeyValue<String, ReplicationMessage> keyValue = it.next();
                    entries++;
                    long cachedAt = (Long) keyValue.value.columnValue(CACHED_AT_KEY);
                    if ((started - cachedAt) >= cacheTimeMs) {
                        possibleExpired.add(keyValue.key);
                    }
                }
            }

            synchronized (sync) {
                for (String key : possibleExpired) {
                    ReplicationMessage message = lookupStore.get(key);
                    if (message == null) continue; // message is deleted
                    long cachedAt = (Long) message.columnValue(CACHED_AT_KEY);
                    if ((started - cachedAt) >= cacheTimeMs) {
                        expiredEntries++;
                        context.forward(key, message.without(CACHED_AT_KEY));
                        lookupStore.delete(key);
                    }
                }
            }
        }

        long duration = System.currentTimeMillis() - started;
        if (entries > 0 && !memoryCache) {
            logger.info("Checked cache {} - {} entries, {} expired entries in {}ms", this.cacheProcName, entries, expiredEntries, duration);
        }
    }

    private void clearPersistentCache() {
        // Make sure all entries from the state store will be evicted
        Set<String> toClear = new HashSet<>();
        try (KeyValueIterator<String, ReplicationMessage> it = lookupStore.all()) {
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> next = it.next();
                context.forward(next.key, next.value.without(CACHED_AT_KEY));
            }
        }
        for (String key : toClear) {
            lookupStore.delete(key);
        }
        clearPersistentCache = false; // one time job

    }

    private class CacheEntry {
        private final long added;
        private final ReplicationMessage entry;

        public CacheEntry(ReplicationMessage entry) {
            this.added = System.currentTimeMillis();
            this.entry = entry;
        }

        public ReplicationMessage getEntry() {
            return entry;
        }

        public boolean isExpired() {
            return (System.currentTimeMillis() - added) > cacheTimeMs;
        }
    }
}