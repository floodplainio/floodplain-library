/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.streams.remotejoin;

import io.floodplain.immutable.api.ImmutableMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessage.Operation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.floodplain.streams.remotejoin.ReplicationTopologyParser.STORE_PREFIX;

// TODO refactor. Remove map usage, replace with non-persistent state store.
public class CacheProcessor implements Processor<String, ReplicationMessage,String, ReplicationMessage> {
    private static final String CACHED_AT_KEY = "_cachedAt";

    private static final Logger logger = LoggerFactory.getLogger(CacheProcessor.class);
    private final Map<String, Record<String,ReplicationMessage>> cache;
    private KeyValueStore<String, ReplicationMessage> lookupStore;
    private ProcessorContext<String,ReplicationMessage> context;
    private final Duration cacheTime;
    private final String cacheProcName;
    private final Object sync = new Object();
    private final boolean memoryCache;
    private boolean clearPersistentCache = false;
    private final int maxSize;

    public CacheProcessor(String cacheProcName, Duration cacheTime, int maxSize, boolean inMemory) {
        this.cacheProcName = cacheProcName;
        this.cacheTime = cacheTime;
        this.cache = new ConcurrentHashMap<>();
        this.memoryCache = inMemory;
        this.maxSize = maxSize;
        logger.info("Using a cache time of {} seconds for {}", cacheTime, cacheProcName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
//        this.startedAt = System.currentTimeMillis();
//        STORE_tenant-deployment-gen-instance-buffer_1_1
        this.lookupStore = (KeyValueStore<String, ReplicationMessage>) context.getStateStore(STORE_PREFIX + cacheProcName);

        long runInterval = Math.max((cacheTime.toMillis() / 10), 1000);

        this.context.schedule(Duration.ofMillis(runInterval), PunctuationType.WALL_CLOCK_TIME, this::checkCache);
        logger.info("Created persistentCache for {} with check interval of {}ms", this.cacheProcName, runInterval);

        if (memoryCache) {
            clearPersistentCache = true;
        }
    }

    @Override
    public void process(Record<String, ReplicationMessage> record) {
        String key = record.key();
        ReplicationMessage message = record.value();
        synchronized (sync) {
            if (message == null || message.operation() == Operation.DELETE) {
                cache.remove(key);
                lookupStore.delete(key);
                context.forward(record);
            } else if (memoryCache) {
                if (cache.size() > maxSize) {
                    logger.warn("Reached max cache size!");
                    context.forward(record);
                } else {
                    cache.put(key, record);
                }
            } else {
                long cachedAt = record.timestamp();
                lookupStore.put(key, message.with(CACHED_AT_KEY, cachedAt, ImmutableMessage.ValueType.LONG));
            }
        }
    }

    @Override
    public void close() {
        synchronized (sync) {
            for (Map.Entry<String, Record<String,ReplicationMessage>> entry : cache.entrySet()) {
                context.forward(entry.getValue());
                cache.remove(entry.getKey());
            }
        }
    }

    public void checkCache(long ms) {
        if (clearPersistentCache) {
            clearPersistentCache();
        }
        int entries = 0;
        int expiredEntries = 0;
        if (memoryCache) {
            Set<String> toForward = new HashSet<>();
            for (Map.Entry<String, Record<String,ReplicationMessage>> entry : cache.entrySet()) {
                if (isExpired(ms,entry.getValue())) {
                    toForward.add(entry.getKey());
                }
            }
            synchronized (sync) {
                for (String key : toForward) {
                    if (cache.get(key) == null) continue; // message is deleted
                    context.forward(cache.get(key));
                    cache.remove(key);
                }
            }
        } else {
            Set<String> possibleExpired = new HashSet<>();
            KeyValueIterator<String,ReplicationMessage> it = lookupStore.all();
            while (it.hasNext()) {
                KeyValue<String, ReplicationMessage> keyValue = it.next();
                entries++;
                long cachedAt = (Long) keyValue.value.value(CACHED_AT_KEY).orElse(0L);
                if ((ms - cachedAt) >= cacheTime.toMillis()) {
                    possibleExpired.add(keyValue.key);
                }
            }

            synchronized (sync) {
                for (String key : possibleExpired) {
                    ReplicationMessage message = lookupStore.get(key);
                    if (message == null ) continue; // message is deleted
                    long cachedAt = (Long) message.value(CACHED_AT_KEY).orElse(0L);
                    if ((ms - cachedAt) >= cacheTime.toMillis()) {
                        expiredEntries++;
                        context.forward(new Record<>(key, message.without(CACHED_AT_KEY), cachedAt));
                        lookupStore.delete(key);
                    }
                }
            }
        }
        // This branch is only for in-memory
        long duration = System.currentTimeMillis() - ms;
        if (entries > 0) {
            logger.info("Checked cache {} - {} entries, {} expired entries in {}ms", this.cacheProcName, entries, expiredEntries, duration);
        }
    }

    private void clearPersistentCache() {
        // Make sure all entries from the state store will be evicted
        Set<String> toClear = new HashSet<>();
        KeyValueIterator<String, ReplicationMessage> it = lookupStore.all();
        while (it.hasNext()) {
            KeyValue<String, ReplicationMessage> next = it.next();
            context.forward(new Record<>(next.key, next.value.without(CACHED_AT_KEY), next.value.timestamp()));
            toClear.add(next.key);
        }
        for (String key : toClear) {
            lookupStore.delete(key);
        }
        clearPersistentCache = false; // one time job
    }

    private boolean isExpired(long timestamp, Record<String,ReplicationMessage> record) {
        return (timestamp - record.timestamp()) > cacheTime.toMillis();
    }

}
