package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.operator.scan.impl.SimpleBlockCacheManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class BlockCacheManagerSizeTest {
    private Random random = new Random();
    private ExecutionContext context = new ExecutionContext();
    private BlockCacheManager<Block> blockCacheManager;

    @Test
    public void sizeTest() {
        final int chunkLimit = 1000;
        final int rowCountInGroup = 4500;

        synchronized (BlockCacheManager.class) {
            blockCacheManager = new SimpleBlockCacheManager();
            blockCacheManager.clear();
            long initialSize = blockCacheManager.getMemorySize();

            final Path path = new Path("foo");
            Block block = createBlock(chunkLimit);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 0, 0,
                0, 1000
            );
            Assert.assertEquals(blockCacheManager.getMemorySize(), initialSize);

            block = createBlock(chunkLimit);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 0, 0,
                1000, 1000
            );
            Assert.assertEquals(blockCacheManager.getMemorySize(), initialSize);

            block = createBlock(chunkLimit);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 0, 0,
                2000, 1000
            );
            Assert.assertEquals(blockCacheManager.getMemorySize(), initialSize);

            block = createBlock(chunkLimit);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 0, 0,
                3000, 1000
            );
            Assert.assertEquals(blockCacheManager.getMemorySize(), initialSize);

            block = createBlock(500);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 0, 0,
                4000, 500
            );
            long lastSize = blockCacheManager.getMemorySize();

            block = createBlock(1000);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 1, 0,
                0, 1000
            );
            Assert.assertEquals(blockCacheManager.getMemorySize(), lastSize);

            block = createBlock(1000);
            blockCacheManager.putCache(
                block, chunkLimit, rowCountInGroup,
                path, 0, 1, 0,
                1000, 1000
            );
            Assert.assertEquals(blockCacheManager.getMemorySize(), lastSize);
        }
    }

    private Block createBlock(int positionCount) {
        BlockBuilder blockBuilder = BlockBuilders.create(DataTypes.LongType, context, positionCount);
        for (int i = 0; i < positionCount; i++) {
            blockBuilder.writeLong(random.nextLong());
        }
        return blockBuilder.build();
    }
}
