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
package io.floodplain.streams.base;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RocksDBConfigurationSetter implements RocksDBConfigSetter {
    private final static Logger logger = LoggerFactory.getLogger(RocksDBConfigurationSetter.class);

    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024L;
    private static final long DEFAULT_BLOCK_CACHE_SIZE = 5 * 1024 * 1024L;
    private static final long DEFAULT_BLOCK_SIZE = 4096L;

    private static final int DEFAULT_L0_FILES = 10;
    private static final int DEFAULT_STATS_DUMP_PERIOD_SEC = 3600;
    private static final int BITS_PER_KEY = 10;


    @Override
    public void setConfig(final String storeName, Options options, Map<String, Object> configs) {
        options.setWriteBufferSize(getWriteBufferSize());
        // Not used in Universal compaction

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig(); // new BlockBasedTableConfig();
        tableConfig.setBlockCacheSize(getBlockCacheSize());
        tableConfig.setBlockSize(getBlockSize());

        if ("true".equalsIgnoreCase(System.getenv("ROCKSDB_USE_BLOOMFILTER"))) {
            tableConfig.setFilter(new BloomFilter(BITS_PER_KEY));
        }
        options.setTableFormatConfig(tableConfig);
        options.setLevel0FileNumCompactionTrigger(getL0NumFiles());
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        options.setStatsDumpPeriodSec(getDumpStatsPeriodSec());
        options.setBaseBackgroundCompactions(2);
        options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);

        options.setLogger(new org.rocksdb.Logger(options) {

            @Override
            protected void log(InfoLogLevel loglevel, String value) {
                logger.debug("storeName: {}, loglevel {}, value {}", storeName, loglevel, value);
            }
        });

    }

    private long getWriteBufferSize() {
        String writeBufferSizeEnv = System.getenv("ROCKSDB_WRITE_BUFFER_SIZE");
        long writeBufferSize = DEFAULT_WRITE_BUFFER_SIZE;
        if (writeBufferSizeEnv != null) {
            try {
                writeBufferSize = Long.parseLong(writeBufferSizeEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse ROCKSDB_WRITE_BUFFER_SIZE ({}) - using default of {}", writeBufferSizeEnv, writeBufferSize, t);
            }
        }
        return writeBufferSize;

    }

    private long getBlockSize() {
        String blockSizeEnv = System.getenv("ROCKSDB_BLOCK_SIZE");
        long blocksize = DEFAULT_BLOCK_SIZE;
        if (blockSizeEnv != null) {
            try {
                blocksize = Long.parseLong(blockSizeEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse ROCKSDB_BLOCK_SIZE ({}) - using default of {}", blockSizeEnv, blockSizeEnv, t);
            }
        }
        return blocksize;

    }

    private long getBlockCacheSize() {
        String blockCacheSizeEnv = System.getenv("ROCKSDB_BLOCK_CACHE_SIZE");
        long blockcachesize = DEFAULT_BLOCK_CACHE_SIZE;
        if (blockCacheSizeEnv != null) {
            try {
                blockcachesize = Long.parseLong(blockCacheSizeEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse ROCKSDB_BLOCK_CACHE_SIZE ({}) - using default of {}", blockCacheSizeEnv, blockcachesize, t);
            }
        }
        return blockcachesize;

    }

    private int getL0NumFiles() {
        String l0numfilesEnv = System.getenv("ROCKSDB_L0_NUMFILES");
        int l0numfiles = DEFAULT_L0_FILES;
        if (l0numfilesEnv != null) {
            try {
                l0numfiles = Integer.parseInt(l0numfilesEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse ROCKSDB_L0_NUMFILES ({}) - using default of {}", l0numfilesEnv, l0numfiles, t);
            }
        }
        return l0numfiles;

    }

    private int getDumpStatsPeriodSec() {
        String dumpperiodsecEnv = System.getenv("ROCKSDB_DUMP_PERIOD_SEC");
        int dumpperiodsec = DEFAULT_STATS_DUMP_PERIOD_SEC;
        if (dumpperiodsecEnv != null) {
            try {
                dumpperiodsec = Integer.parseInt(dumpperiodsecEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse ROCKSDB_DUMP_PERIOD_SEC ({}) - using default of {}", dumpperiodsecEnv, dumpperiodsec, t);
            }
        }
        return dumpperiodsec;

    }


}
