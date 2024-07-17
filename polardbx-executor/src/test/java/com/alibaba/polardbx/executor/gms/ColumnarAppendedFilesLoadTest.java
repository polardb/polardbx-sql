package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

import static com.alibaba.polardbx.executor.gms.FileVersionStorage.CSV_CHUNK_LIMIT;

public class ColumnarAppendedFilesLoadTest extends FileVersionStorageTestBase {

    @Test
    public void testCsvLoad() {
        int loadedVersionCnt = 0;
        for (MockAppendedFilesStatus status : CSV_STATUSES) {
            List<Chunk> chunkList = fileVersionStorage.csvData(status.checkpointTso, CSV_FILE_NAME);
            Assert.assertEquals(status.totalRows, chunkList.stream().mapToLong(Chunk::getPositionCount).sum());
            // Random check an older version
            if (loadedVersionCnt > 0) {
                int version = R.nextInt(loadedVersionCnt);
                chunkList = fileVersionStorage.csvData(CSV_STATUSES[version].checkpointTso, CSV_FILE_NAME);
                Assert.assertEquals(
                    CSV_STATUSES[version].totalRows,
                    chunkList.stream().mapToLong(Chunk::getPositionCount).sum()
                );
            }
            loadedVersionCnt++;
        }
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
            List<Chunk> chunkList = fileVersionStorage.csvData(status.checkpointTso, CSV_FILE_NAME);

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
    public void testCsvDataWithoutCache() {
        for (MockAppendedFilesStatus status : CSV_STATUSES) {
            // Check for csv read bypass for columnar checksum
            List<Chunk> chunkList =
                fileVersionStorage.csvRawOrcTypeData(status.checkpointTso, CSV_FILE_NAME, new ExecutionContext());
            Assert.assertEquals(status.totalRows, chunkList.stream().mapToLong(Chunk::getPositionCount).sum());
        }
    }

    @Test
    public void testGenerateDeleteBitmap() {
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
