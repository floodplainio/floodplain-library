package io.floodplain.streams.base;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BoundedMemoryRocksDBConfig implements RocksDBConfigSetter {

    private static final long BLOCK_SIZE = 1000;
    private static final long DEFAULT_TOTAL_OFF_HEAP_MEMORY = 50_000_000;
    private static final double INDEX_FILTER_BLOCK_RATIO = 0.5;
    private static final int N_MEMTABLES = 100;
    private static final long MEMTABLE_SIZE = 20000;
    // See #1 below
    private static org.rocksdb.Cache cache = new org.rocksdb.LRUCache(totalOffHeapMemory(), -1, false, INDEX_FILTER_BLOCK_RATIO);
    private static final long DEFAULT_TOTAL_MEMTABLE_MEMORY = 50_000_000;
    private static org.rocksdb.WriteBufferManager writeBufferManager = new org.rocksdb.WriteBufferManager(totalMemtableMemory(), cache);

    private final static Logger logger = LoggerFactory.getLogger(RocksDBConfigurationSetter.class);

    private static long totalOffHeapMemory() {
        String offHeapTotalEnv = System.getenv("TOTAL_OFF_HEAP_MEMORY");
        long offHeapTotal = DEFAULT_TOTAL_OFF_HEAP_MEMORY;
        if (offHeapTotalEnv != null) {
            try {
                offHeapTotal = Long.parseLong(offHeapTotalEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse ROCKSDB_WRITE_BUFFER_SIZE ({}) - using default of {}", offHeapTotalEnv, offHeapTotal, t);
            }
        }
        return offHeapTotal;
    }

    private static long totalMemtableMemory() {
        String totalMemtableMemoryEnv = System.getenv("TOTAL_OFF_HEAP_MEMORY");
        long totalMemtableMemory = DEFAULT_TOTAL_MEMTABLE_MEMORY;
        if (totalMemtableMemoryEnv != null) {
            try {
                totalMemtableMemory = Long.parseLong(totalMemtableMemoryEnv.trim());
            } catch (Throwable t) {
                logger.warn("Unable to parse TOTAL_OFF_HEAP_MEMORY ({}) - using default of {}", totalMemtableMemoryEnv, totalMemtableMemory, t);
            }
        }
        return totalMemtableMemory;
    }


    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {

        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();

        // These three options in combination will limit the memory used by RocksDB to the size passed to the block cache (TOTAL_OFF_HEAP_MEMORY)
        tableConfig.setBlockCache(cache);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        options.setWriteBufferManager(writeBufferManager);

        // These options are recommended to be set when bounding the total memory
        // See #2 below
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        tableConfig.setPinTopLevelIndexAndFilter(true);
        // See #3 below
        tableConfig.setBlockSize(BLOCK_SIZE);
        options.setMaxWriteBufferNumber(N_MEMTABLES);
        options.setWriteBufferSize(MEMTABLE_SIZE);

        options.setTableFormatConfig(tableConfig);
    }

    @Override
    public void close(final String storeName, final Options options) {
        // Cache and WriteBufferManager should not be closed here, as the same objects are shared by every store instance.
    }
}