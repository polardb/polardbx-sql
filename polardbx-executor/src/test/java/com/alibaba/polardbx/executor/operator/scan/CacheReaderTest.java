package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.operator.scan.impl.CacheReaderImpl;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class CacheReaderTest extends ScanTestBase {

    @Test
    public void testCacheReader() throws IOException {
        // [stripe 0]
        // Stripe: offset: 3 data: 6333874 rows: 240000 tail: 111 index: 4025
        final int stripeId = 0;
        final int columnId = 2;
        final int[] rowGroupIds = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22};

        final long rowsInStripe = orcTail.getStripes().get(stripeId).getNumberOfRows();
        int groupsInStripe = (int) ((rowsInStripe + indexStride - 1) / indexStride);

        BlockCacheManager<Block> blockCacheManager =
            prepareCache(stripeId, new int[] {columnId}, rowGroupIds);

        CacheReader<Block> cacheReader = new CacheReaderImpl(stripeId, columnId, groupsInStripe);

        if (!cacheReader.isInitialized()) {
            // Check the block-cache-manager what row-group of this column have been cached.
            Map<Integer, SeekableIterator<Block>> caches = blockCacheManager.getCachedRowGroups(
                FILE_PATH, stripeId, columnId,
                fromRowGroupIds(stripeId, rowGroupIds)
            );
            cacheReader.initialize(caches);
        }

        // Check cached row group bitmap.
        boolean[] bitmap = cacheReader.cachedRowGroupBitmap();
        Assert.assertTrue(
            Arrays.equals(fromRowGroupIds(stripeId, rowGroupIds), bitmap));

        // collect block locations in this group.
        List<BlockLocation> locationList = new ArrayList<>();
        for (int groupId : rowGroupIds) {
            final int rowCountInGroup = getRowCount(orcTail.getStripes().get(stripeId), groupId);
            for (int startPosition = 0; startPosition < rowCountInGroup; startPosition += DEFAULT_CHUNK_LIMIT) {
                int positionCount = Math.min(DEFAULT_CHUNK_LIMIT, rowCountInGroup - startPosition);
                locationList.add(new BlockLocation(groupId, startPosition, positionCount));
            }
        }

        // Check block existence
        for (BlockLocation location : locationList) {
            Block cached = cacheReader.getCache(location.rowGroupId, location.startPosition);
            Assert.assertNotNull(cached);
            Assert.assertEquals(location.positionCount, cached.getPositionCount());
        }
    }

    @Test
    public void testForbidBlockCache() throws IOException {
        // [stripe 0]
        // Stripe: offset: 3 data: 6333874 rows: 240000 tail: 111 index: 4025
        final int stripeId = 0;
        final int columnId = 2;
        final int[] rowGroupIds = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22};

        final long rowsInStripe = orcTail.getStripes().get(stripeId).getNumberOfRows();
        int groupsInStripe = (int) ((rowsInStripe + indexStride - 1) / indexStride);

        CacheReader<Block> cacheReader = new CacheReaderImpl(stripeId, columnId, groupsInStripe);

        // Initialize cache reader with empty map to forbid the block cache.
        cacheReader.initialize(new HashMap<>());

        Assert.assertTrue(cacheReader.isInitialized());

        // Check cached row group bitmap.
        boolean[] bitmap = cacheReader.cachedRowGroupBitmap();
        for (boolean b : bitmap) {
            Assert.assertFalse(b);
        }

        // collect block locations in this group.
        List<BlockLocation> locationList = new ArrayList<>();
        for (int groupId : rowGroupIds) {
            final int rowCountInGroup = getRowCount(orcTail.getStripes().get(stripeId), groupId);
            for (int startPosition = 0; startPosition < rowCountInGroup; startPosition += DEFAULT_CHUNK_LIMIT) {
                int positionCount = Math.min(DEFAULT_CHUNK_LIMIT, rowCountInGroup - startPosition);
                locationList.add(new BlockLocation(groupId, startPosition, positionCount));
            }
        }

        // Check block existence
        for (BlockLocation location : locationList) {
            Block cached = cacheReader.getCache(location.rowGroupId, location.startPosition);
            Assert.assertNull(cached);
        }
    }

    protected Block createRandomBlock(Random random, ExecutionContext context, int positionCount) {
        BlockBuilder blockBuilder = BlockBuilders.create(DataTypes.LongType, context, positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.writeLong(random.nextLong());
        }
        return blockBuilder.build();
    }
}
