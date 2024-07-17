package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class FloatColumnReaderTest extends FullTypeScanTestBase {

    private static final int COLUMN_ID = 4;

    @Test
    public void testSingleGroup() throws IOException {
        final int columnId = COLUMN_ID + 1;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
//        columnIncluded[0] = true;
        columnIncluded[columnId] = true;
        doTest(0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0}),
            ImmutableList.of(
                new ScanTestBase.BlockLocation(0, 0, 100),
                new ScanTestBase.BlockLocation(0, 400, 300)
            ));
    }

    @Test
    public void testMultiGroup() throws IOException {
        final int columnId = COLUMN_ID + 1;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
//        columnIncluded[0] = true;
        columnIncluded[columnId] = true;
        doTest(0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new ScanTestBase.BlockLocation(0, 0, 1000),
                new ScanTestBase.BlockLocation(1, 1000, 700),
                new ScanTestBase.BlockLocation(2, 2000, 500)
            )
        );
    }

    @Override
    protected void check(Block block, VectorizedRowBatch batch, int targetColumnId) {
        Assert.assertTrue(block instanceof FloatBlock);

        for (int row = 0; row < batch.size; row++) {
            ColumnVector vector = batch.cols[COLUMN_ID];

            if (vector.isNull[row]) {
                // check null
                Assert.assertTrue(block.isNull(row));
            } else {
                Assert.assertTrue(vector instanceof DoubleColumnVector);
                double[] array = ((DoubleColumnVector) vector).vector;
                int idx = row;
                if (vector.isRepeating) {
                    idx = 0;
                }
                Assert.assertEquals(array[idx], block.getFloat(row), 0E-10);
            }
        }
    }
}
