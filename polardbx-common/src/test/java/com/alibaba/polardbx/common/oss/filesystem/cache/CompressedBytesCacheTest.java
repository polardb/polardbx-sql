package com.alibaba.polardbx.common.oss.filesystem.cache;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.google.common.cache.Cache;
import io.airlift.slice.SizeOf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Random;

import static com.google.common.base.Verify.verify;

public class CompressedBytesCacheTest {
    public static final int CHAR_SIZE = 1024 * 1024;
    public static final Random RANDOM = new Random();

    private Configuration configuration;
    private FileSystem localFileSystem;

    private final String RANDOM_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private final File spillPath = Paths.get("./tmp/" + this.getClass().getSimpleName()).toAbsolutePath().toFile();
    private final File dataPath = Paths.get("./data/" + this.getClass().getSimpleName()).toAbsolutePath().toFile();

    private final File dataFile1 = new File(dataPath, "data_1");
    private final File dataFile2 = new File(dataPath, "data_2");
    private final File dataFile3 = new File(dataPath, "data_3");
    private final File dataFile4 = new File(dataPath, "data_4");
    private final File dataFile5 = new File(dataPath, "data_5");
    private final File[] dataFiles = new File[] {
        dataFile1, dataFile2, dataFile3, dataFile4, dataFile5
    };

    public final char[] chars = new char[CHAR_SIZE];

    private CacheManager cacheManager;

    @Before
    public void initializeCacheManager() throws IOException {
        if (!dataPath.exists()) {
            dataPath.mkdirs();
        }
        configuration = new Configuration();
        configuration.setBoolean("fs.file.impl.disable.cache", true);

        localFileSystem = FileSystem.get(new Path(dataPath.getAbsolutePath()).toUri(), configuration);

        Engine engine = Engine.LOCAL_DISK;

        FileConfig fileConfig = FileConfig.getInstance();
        fileConfig.loadValue(null, ConnectionProperties.OSS_FS_MAX_CACHED_ENTRIES, "4");
        fileConfig.loadValue(null, ConnectionProperties.OSS_FS_USE_BYTES_CACHE, "true");
        fileConfig.loadValue(null, ConnectionProperties.OSS_FS_MEMORY_RATIO_OF_BYTES_CACHE, "0.3");
        fileConfig.loadValue(null, ConnectionProperties.MPP_SPILL_PATHS, spillPath.getAbsolutePath());

        CacheConfig cacheConfig = fileConfig.getCacheConfig();

        FileMergeCacheConfig fileMergeCacheConfig = fileConfig.getMergeCacheConfig();

        cacheManager = FileMergeCacheManager.createMergeCacheManager(engine, cacheConfig, fileMergeCacheConfig);
    }

    @Before
    public void initializeFiles() throws IOException {
        for (File file : dataFiles) {
            writeFile(file);
        }
    }

    @Test
    public void testSingleFile() throws IOException, InterruptedException {
        final int length = 1024;
        final int position = 2048;

        doTestRead(dataFile1, length, position);

        long maxSizeOfCompressedBytes =
            ((FileMergeCacheManager) cacheManager).getMaxSizeOfCompressedBytes();

        Cache<FileMergeCacheManager.LocalCacheFile, byte[]> compressedBytesCache =
            ((FileMergeCacheManager) cacheManager).getCompressedBytesCache();

        CacheStats cacheStats =
            ((FileMergeCacheManager) cacheManager).getCompressedBytesCacheStats();

        // The writing cache action is asynchronous.
        Thread.sleep(2000);

        Assert.assertTrue(compressedBytesCache.size() == 1);
        Assert.assertTrue(cacheStats.getInMemoryRetainedBytes()
            == length + FileMergeCacheManager.LocalCacheFile.BASE_SIZE_IN_BYTES + 16);
    }

    @Test
    public void testEvict() throws IOException, InterruptedException {

        final int length = 1024;

        doTestRead(dataFile1, length, 1024 * 1);
        doTestRead(dataFile2, length, 1024 * 2 + 100);
        doTestRead(dataFile3, length, 1024 * 3 + 99);
        doTestRead(dataFile4, length, 1024 * 4 + 88);
        doTestRead(dataFile5, length, 1024 * 5 + 77);

        Cache<FileMergeCacheManager.LocalCacheFile, byte[]> compressedBytesCache =
            ((FileMergeCacheManager) cacheManager).getCompressedBytesCache();

        CacheStats cacheStats =
            ((FileMergeCacheManager) cacheManager).getCompressedBytesCacheStats();

        // The writing cache action is asynchronous.
        Thread.sleep(2000);

        // Evicted 1 file.
        Assert.assertTrue(compressedBytesCache.size() == 4);
        Assert.assertTrue(cacheStats.getInMemoryRetainedBytes()
            == (length + FileMergeCacheManager.LocalCacheFile.BASE_SIZE_IN_BYTES + 16) * 4);
    }

    @Test
    public void testMerge() throws IOException, InterruptedException {

        final int length = 1024;

        doTestRead(dataFile1, length, 1024 * 1); // file1: [1024, 2048)
        doTestRead(dataFile2, length, 1024 * 3 + 99); // file2: [3171, 4195)

        doTestRead(dataFile1, length, 1024 * 2 - 100); // file1: [1948, 2972)
        doTestRead(dataFile2, length, 1024 * 4 - 88); // file2: [4088, 5032)

        doTestRead(dataFile3, length, 1024 * 5 + 77); // file3: [5197, 6221)

        Cache<FileMergeCacheManager.LocalCacheFile, byte[]> compressedBytesCache =
            ((FileMergeCacheManager) cacheManager).getCompressedBytesCache();

        CacheStats cacheStats =
            ((FileMergeCacheManager) cacheManager).getCompressedBytesCacheStats();

        // The writing cache action is asynchronous.
        Thread.sleep(2000);

        // 3 files have been written.
        Assert.assertTrue(compressedBytesCache.size() == 3);

        // data file 1 has been merged: [1024, 2972)
        // data file 2 has been merged: [3171, 5032)
        // data file 3 is single: [5197, 6221)
        Assert.assertTrue(cacheStats.getInMemoryRetainedBytes()
            == ((2972 - 1024) + FileMergeCacheManager.LocalCacheFile.BASE_SIZE_IN_BYTES + 16) +
            ((5032 - 3171) + FileMergeCacheManager.LocalCacheFile.BASE_SIZE_IN_BYTES + 16) +
            (length + FileMergeCacheManager.LocalCacheFile.BASE_SIZE_IN_BYTES + 16)
        );
    }

    private void doTestRead(File file, int length, int position) throws IOException {

        Path filePath = new Path(file.getAbsolutePath());

        FSDataInputStream fileInputStream = localFileSystem.open(filePath);
        FileMergeCachingInputStream inputStream = new FileMergeCachingInputStream(
            fileInputStream,
            cacheManager,
            filePath,
            cacheManager.getMaxCacheQuota(), false
        );

        // read from cache manager
        byte[] buffer = new byte[length];
        inputStream.readFully(position, buffer);

        // read from physical file
        byte[] validationBuffer = new byte[length];
        fileInputStream.readFully(position, validationBuffer, 0, length);

        // verify the bytes
        for (int i = 0; i < length; i++) {
            verify(buffer[i] == validationBuffer[i], "corrupted buffer at position " + i);
        }
    }

    @After
    public void deleteFiles() {
        for (File file : dataFiles) {
            deleteFile(file);
        }
    }

    private void writeFile(File dataFile) throws IOException {
        if (!dataPath.exists()) {
            dataPath.mkdirs();
        }
        if (!dataFile.exists()) {
            dataFile.createNewFile();
        }

        try (FileWriter fileWriter = new FileWriter(dataFile)) {
            for (int i = 0; i < CHAR_SIZE; i++) {
                chars[i] = RANDOM_STRING.charAt(RANDOM.nextInt(RANDOM_STRING.length()));
            }
            fileWriter.write(chars);
            fileWriter.flush();
        }
    }

    private void deleteFile(File dataFile) {
        if (dataFile.exists()) {
            dataFile.delete();
        }
        Assert.assertTrue(!dataFile.exists());
    }

}
