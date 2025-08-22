package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;
import java.util.List;

import static com.alibaba.polardbx.executor.gms.FileVersionStorage.CSV_CHUNK_LIMIT;

public class ColumnarAppendedFilesLoadTest extends FileVersionStorageTestBase {

    @Test
    public void testCsvLoad() throws InterruptedException {
        int loadedVersionCnt = 0;
        for (MockAppendedFilesStatus status : CSV_STATUSES) {
            List<Chunk> chunkList =
                fileVersionStorage.csvData(status.checkpointTso, status.checkpointTso, CSV_FILE_NAME);
            Assert.assertEquals(status.totalRows, chunkList.stream().mapToLong(Chunk::getPositionCount).sum());
            // Random check an older version
            if (loadedVersionCnt > 0) {
                int version = R.nextInt(loadedVersionCnt);
                chunkList =
                    fileVersionStorage.csvData(CSV_STATUSES[version].checkpointTso, CSV_STATUSES[version].checkpointTso,
                        CSV_FILE_NAME);
                Assert.assertEquals(
                    CSV_STATUSES[version].totalRows,
                    chunkList.stream().mapToLong(Chunk::getPositionCount).sum()
                );
            }
            loadedVersionCnt++;
        }
        Assert.assertTrue(fileVersionStorage.getCsvCacheSizeInBytes() > 0);
        fileVersionStorage.purgeByFile(CSV_FILE_NAME);
        Thread.sleep(50);
        Assert.assertEquals(0, fileVersionStorage.getCsvCacheSizeInBytes());
    }

    @Test
    public void testCsvLoadWithRandomPurge() {
        Long lastTso = null;
        for (MockAppendedFilesStatus status : CSV_STATUSES) {
            boolean purged = false;
            if (lastTso != null && R.nextInt(3) <= 1) {
                purged = true;
                fileVersionStorage.purge(lastTso);
            }
            List<Chunk> chunkList =
                fileVersionStorage.csvData(status.checkpointTso, status.checkpointTso, CSV_FILE_NAME);

            long totalRows = chunkList.stream().mapToLong(Chunk::getPositionCount).sum();

            // Check row count
            Assert.assertEquals(status.totalRows, totalRows);

            // Check csv chunk size
            Assert.assertTrue(chunkList.stream().allMatch(chunk -> chunk.getPositionCount() > 0));
            Assert.assertTrue(chunkList.stream().allMatch(chunk -> chunk.getPositionCount() <= CSV_CHUNK_LIMIT));
            if (purged) {
                if (totalRows > CSV_STATUSES[FIRST_CSV_PART_OVER_1000].totalRows) {
                    // Check csv purge for each 1000 rows
                    Assert.assertEquals(CSV_CHUNK_LIMIT, chunkList.get(0).getPositionCount());
                    Assert.assertEquals(3, chunkList.size());
                } else {
                    Assert.assertEquals(2, chunkList.size());
                }
            } else {
                if (totalRows > CSV_STATUSES[FIRST_CSV_PART_OVER_1000 + 1].totalRows) {
                    Assert.assertTrue(chunkList.size() > 3);
                } else if (status.checkpointTso > CSV_STATUSES[1].checkpointTso) {
                    Assert.assertTrue(chunkList.size() > 2);
                }
            }

            lastTso = status.checkpointTso;
        }
    }

    @Test
    public void testCsvLoadTsoInverseOrderWithPurge() {
        MockAppendedFilesStatus version3 = CSV_STATUSES[3];
        MockAppendedFilesStatus version4 = CSV_STATUSES[4];
        List<Chunk> chunkList =
            fileVersionStorage.csvData(version4.checkpointTso, version4.checkpointTso, CSV_FILE_NAME);

        long totalRows = chunkList.stream().mapToLong(Chunk::getPositionCount).sum();

        Assert.assertEquals(version4.totalRows, totalRows);

        // This will make tso of version 3 in cache turn to purge tso
        long purgeTso = version3.checkpointTso + 1L;
        fileVersionStorage.purge(purgeTso);

        chunkList = fileVersionStorage.csvData(version3.checkpointTso, purgeTso, CSV_FILE_NAME);

        totalRows = chunkList.stream().mapToLong(Chunk::getPositionCount).sum();

        Assert.assertEquals(version3.totalRows, totalRows);
    }

    @Test
    public void testCsvDataWithoutCache() {
        for (MockAppendedFilesStatus status : CSV_STATUSES) {
            // Check for csv read bypass for columnar checksum
            Iterator<Chunk> iterator =
                MultiVersionCsvData.loadRawOrcTypeUntilTso(CSV_FILE_NAME, new ExecutionContext(),
                    0, (int) (status.appendOffset + status.appendLength));
            long rowCount = 0;
            while (iterator.hasNext()) {
                Chunk chunk = iterator.next();
                rowCount += chunk.getPositionCount();
            }
            Assert.assertEquals(status.totalRows, rowCount);
        }
    }

    @Test
    public void testCsvDataWithoutCache2() {
        for (MockAppendedFilesStatus status : CSV_STATUSES) {
            // Check for csv read bypass for columnar checksum
            Iterator<Chunk> iterator =
                MultiVersionCsvData.loadSpecifiedCsvFile(CSV_FILE_NAME, new ExecutionContext(),
                    0, (int) (status.appendOffset + status.appendLength));
            long rowCount = 0;
            while (iterator.hasNext()) {
                Chunk chunk = iterator.next();
                rowCount += chunk.getPositionCount();
            }
            Assert.assertEquals(status.totalRows, rowCount);
        }

        // For incremental check.
        for (int i = 0; i < CSV_STATUSES.length - 1; i++) {
            // Check for csv read bypass for columnar checksum
            MockAppendedFilesStatus status0 = CSV_STATUSES[i];
            MockAppendedFilesStatus status1 = CSV_STATUSES[i + 1];
            Iterator<Chunk> iterator =
                MultiVersionCsvData.loadSpecifiedCsvFile(CSV_FILE_NAME, new ExecutionContext(),
                    (int) status1.appendOffset, (int) (status1.appendOffset + status1.appendLength));

            long rowCount = 0;
            while (iterator.hasNext()) {
                Chunk chunk = iterator.next();
                rowCount += chunk.getPositionCount();
            }
            Assert.assertEquals(status1.totalRows - status0.totalRows, rowCount);
        }
    }

    @Test
    public void testGenerateDeleteBitmap() throws InterruptedException {
        int loadedVersionCnt = 0;
        for (MockAppendedFilesStatus status : DEL_STATUS) {
            RoaringBitmap bitmap = fileVersionStorage.getDeleteBitMap(FILE_META, status.checkpointTso);
            Assert.assertEquals(status.totalRows, bitmap.getCardinality());
            // Random check an older version
            if (loadedVersionCnt > 0) {
                int version = R.nextInt(loadedVersionCnt);
                bitmap = fileVersionStorage.getDeleteBitMap(FILE_META, DEL_STATUS[version].checkpointTso);
                Assert.assertEquals(DEL_STATUS[version].totalRows, bitmap.getCardinality());
            }
            loadedVersionCnt++;
        }
        Assert.assertTrue(fileVersionStorage.getDelCacheSizeInBytes() > 0);
        fileVersionStorage.purgeByFile(CSV_FILE_NAME);
        Thread.sleep(50);
        Assert.assertEquals(0, fileVersionStorage.getDelCacheSizeInBytes());
    }

    @Test
    public void testGenerateDeleteBitmapWithRandomPurge() {
        Long lastTso = null;
        for (MockAppendedFilesStatus status : DEL_STATUS) {
            if (lastTso != null && R.nextInt(3) <= 1) {
                fileVersionStorage.purge(lastTso);
            }
            RoaringBitmap bitmap = fileVersionStorage.getDeleteBitMap(FILE_META, status.checkpointTso);
            Assert.assertEquals(status.totalRows, bitmap.getCardinality());
            lastTso = status.checkpointTso;
        }
    }
}
