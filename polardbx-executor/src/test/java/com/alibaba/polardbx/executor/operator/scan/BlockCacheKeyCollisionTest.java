package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.impl.SimpleBlockCacheManager;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class BlockCacheKeyCollisionTest {
    @Test
    public void test() {
        // pathId (tableId + fileId) (32bit) + rowGroupId (16bit) + stripeId (4bit) + columnId (12bit).
        // rowGroupId: 0 ~ 65535
        // stripeId: 0 ~ 15
        // columnId: 0 ~ 4095
        int[] rowGroupIds = {0, 100, 5555, 9999, 57489, 65535};

        Path path = new Path("file:///tmp/test_53452958392593.orc");

        LongOpenHashSet resultSet = new LongOpenHashSet();

        for (int rowGroupId : rowGroupIds) {
            for (int stripeId = 0; stripeId < 16; stripeId++) {
                for (int columnId = 0; columnId < 4096; columnId++) {
                    long cacheKey = SimpleBlockCacheManager.buildBlockCacheKey(path, stripeId, columnId, rowGroupId);

                    Assert.assertFalse(resultSet.contains(cacheKey));
                    resultSet.add(cacheKey);
                }
            }
        }
    }
}
