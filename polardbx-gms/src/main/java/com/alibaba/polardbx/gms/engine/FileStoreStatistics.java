package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.Constants;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.StorageStatistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileStoreStatistics {
    public static long STATS_TIME_PERIOD = 5 * 1000;

    public static final ScheduledExecutorService SCHEDULER =
        Executors.newScheduledThreadPool(1, new NamedThreadFactory("File-Store-Stats", true));
    public static final MatrixStatistics EMPTY = new MatrixStatistics(false);

    public static final String PROPERTY_STATS_TIME_PERIOD = "STATS_TIME_PERIOD";

    private static final List<Map<StatisticItem, String>> PREVIOUS_STATS = new ArrayList<>();

    private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();

    private static final int FILE_STORAGE_FIELD_COUNT = 8;

    static {
        if (System.getProperty(PROPERTY_STATS_TIME_PERIOD) != null) {
            STATS_TIME_PERIOD = Long.valueOf(System.getProperty("STATS_TIME_PERIOD"));
        }

        SCHEDULER.scheduleWithFixedDelay(
            () -> {
                // collect statistics data.
                collectStatistics();
            }, 0, STATS_TIME_PERIOD, TimeUnit.MILLISECONDS);
    }

    private static void collectStatistics() {
        List<Map<StatisticItem, String>> localStats = new ArrayList<>();
        for (Engine engine : Engine.values()) {
            // for each file store engine
            if (!Engine.isFileStore(engine)) {
                continue;
            }

            FileSystemGroup group = FileSystemManager.getFileSystemGroup(engine);
            if (group != null) {
                FileSystem master = group.getMaster();
                // do collect
                foreachFileSystem(master, engine, true, localStats);

                for (FileSystem slave : group.getSlaves()) {
                    // do collect
                    foreachFileSystem(slave, engine, false, localStats);
                }
            }
        }

        // replace with new stats.
        Lock writeLock = LOCK.writeLock();
        try {
            writeLock.lock();
            PREVIOUS_STATS.clear();
            PREVIOUS_STATS.addAll(localStats);
        } finally {
            writeLock.unlock();
        }
    }

    private static void foreachFileSystem(FileSystem fileSystem, Engine engine, boolean isMaster, List<Map<StatisticItem, String>> localStats) {
        ImmutableMap.Builder<StatisticItem, String> statsBuilder = ImmutableMap.builder();
        // basic info.
        String endpoint = fileSystem.getConf().get(Constants.ENDPOINT_KEY);

        statsBuilder.put(StatisticItem.ENGINE, String.valueOf(engine));
        statsBuilder.put(StatisticItem.ENDPOINT, endpoint == null ? "" : endpoint);
        statsBuilder.put(StatisticItem.FILE_URI, fileSystem.getWorkingDirectory().toString());
        statsBuilder.put(StatisticItem.IS_MASTER, String.valueOf(isMaster ? 1 : 0));

        // statistics info.
        String scheme = fileSystem.getScheme();
        if (scheme == null) {
            return;
        }
        GlobalStorageStatistics globalStorageStatistics = FileSystem.getGlobalStorageStatistics();
        StorageStatistics storageStatistics = globalStorageStatistics.get(scheme);
        if (storageStatistics == null) {
            return;
        }

        // record statistics info in the last period
        Iterator<StorageStatistics.LongStatistic> iterator = storageStatistics.getLongStatistics();
        while (iterator.hasNext()) {
            StorageStatistics.LongStatistic statistics = iterator.next();
            StatisticItem item = StatisticItem.of(statistics.getName());
            if (item == null) {
                continue;
            }
            statsBuilder.put(item, String.valueOf(statistics.getValue()));
        }

        localStats.add(statsBuilder.build());

        // reset all statistics value to 0.
        storageStatistics.reset();
    }

    public enum StatisticItem {
        ENGINE("engine"),
        ENDPOINT("endpoint"),
        FILE_URI("file_uri"),
        IS_MASTER("is_master"),
        BYTES_READ("bytesRead"),
        BYTES_WRITTEN("bytesWritten"),
        READ_OPS("readOps"),
        WRITE_OPS("writeOps");

        private String itemName;

        StatisticItem(String itemName) {
            this.itemName = itemName;
        }

        static StatisticItem of(String itemName) {
            for (StatisticItem item : values()) {
                if (item.itemName.equalsIgnoreCase(itemName)) {
                    return item;
                }
            }
            return null;
        }
    }

    public static Iterator<Map<StatisticItem, String>> currentStats() {
        return PREVIOUS_STATS.iterator();
    }

    public synchronized static List<byte[][]> generateFileStoragePacket() {
        if (PREVIOUS_STATS.isEmpty()) {
            // force collection.
            collectStatistics();
        }

        List<byte[][]> fileStorageRows = new ArrayList<>();

        Lock readLock = LOCK.readLock();
        try {
            readLock.lock();
            Iterator<Map<FileStoreStatistics.StatisticItem, String>> iterator = currentStats();
            while (iterator.hasNext()) {
                Map<FileStoreStatistics.StatisticItem, String> statsMap = iterator.next();

                byte[][] result = new byte[FILE_STORAGE_FIELD_COUNT][];
                int pos = 0;

                // basic info.
                result[pos++] = bytesOfString(statsMap.get(FileStoreStatistics.StatisticItem.ENGINE));
                result[pos++] = bytesOfString(statsMap.get(FileStoreStatistics.StatisticItem.ENDPOINT));
                result[pos++] = bytesOfString(statsMap.get(FileStoreStatistics.StatisticItem.FILE_URI));
                result[pos++] = bytesOfString(statsMap.get(FileStoreStatistics.StatisticItem.IS_MASTER));

                // statistics info.
                result[pos++] = bytesOfStats(statsMap.get(FileStoreStatistics.StatisticItem.BYTES_READ));
                result[pos++] = bytesOfStats(statsMap.get(FileStoreStatistics.StatisticItem.BYTES_WRITTEN));
                result[pos++] = bytesOfStats(statsMap.get(FileStoreStatistics.StatisticItem.READ_OPS));
                result[pos++] = bytesOfStats(statsMap.get(FileStoreStatistics.StatisticItem.WRITE_OPS));

                fileStorageRows.add(result);
            }
            return fileStorageRows;
        } finally {
            readLock.unlock();
        }
    }

    private static byte[] bytesOfStats(String value) {
        if (TStringUtil.isEmpty(value)) {
            return new byte[] {};
        }
        long stat = Long.valueOf(value);
        stat /= (FileStoreStatistics.STATS_TIME_PERIOD / 1000);
        return bytesOfString(String.valueOf(stat));
    }

    private static byte[] bytesOfString(String value) {
        return TStringUtil.isEmpty(value) ? new byte[] {} : value.getBytes();
    }
}
