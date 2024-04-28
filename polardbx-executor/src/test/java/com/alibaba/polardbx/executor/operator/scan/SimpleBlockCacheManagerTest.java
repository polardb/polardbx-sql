package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

public class SimpleBlockCacheManagerTest {
    private Random random = new Random();
    private ExecutionContext context = new ExecutionContext();

    @Test
    public void testOneRowGroup() {
        final int chunkLimit = 1000;
        BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();

        // It means 3 blocks in one row-group.
        final Path path = new Path("foo");
        final int rowCountInGroup = 2900;
        final int stripeId = 0;
        final int rowGroupId = 0;
        final int columnId = 0;
        final boolean rowGroupIncluded[] = new boolean[1];
        rowGroupIncluded[0] = true;

        // write range [0, 1000)
        int position = 0;
        int positionCount = 1000;
        Block block = createBlock(positionCount);
        blockCacheManager.putCache(
            block, chunkLimit, rowCountInGroup,
            path, stripeId, rowGroupId, columnId,
            position, positionCount
        );

        // Not visible.
        Map<Integer, SeekableIterator<Block>> cachedBlocks =
            blockCacheManager.getCachedRowGroups(path, stripeId, columnId, rowGroupIncluded);
        Assert.assertTrue(cachedBlocks.isEmpty());

        // write range [1000, 2000)
        position += positionCount;
        positionCount = 1000;
        block = createBlock(positionCount);
        blockCacheManager.putCache(
            block, chunkLimit, rowCountInGroup,
            path, stripeId, rowGroupId, columnId,
            position, positionCount
        );

        // Not visible.
        cachedBlocks = blockCacheManager.getCachedRowGroups(path, stripeId, columnId, rowGroupIncluded);
        Assert.assertTrue(cachedBlocks.isEmpty());

        // write range [2000, 2900)
        position += positionCount;
        positionCount = 900;
        block = createBlock(positionCount);
        blockCacheManager.putCache(
            block, chunkLimit, rowCountInGroup,
            path, stripeId, rowGroupId, columnId,
            position, positionCount
        );

        // Test seekable
        cachedBlocks = blockCacheManager.getCachedRowGroups(path, stripeId, columnId, rowGroupIncluded);
        SeekableIterator<Block> blockIterator = cachedBlocks.get(rowGroupId);
        Block cached = blockIterator.seek(2000 + 1);
        Assert.assertTrue(cached.getPositionCount() == 900);
    }

    private Block createBlock(int positionCount) {
        BlockBuilder blockBuilder = BlockBuilders.create(DataTypes.LongType, context, positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.writeLong(random.nextLong());
        }
        return blockBuilder.build();
    }
}
