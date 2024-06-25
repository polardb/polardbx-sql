package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class NormalDecimalColumnReaderTest extends FullTypeScanTestBase {

    private static final int COLUMN_ID = 9;

    @Test
    public void testSingleGroup() throws IOException {
        final int columnId = COLUMN_ID + 1;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
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
        columnIncluded[0] = true;
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
        Assert.assertTrue(block instanceof DecimalBlock);
        Assert.assertFalse(((DecimalBlock) block).isDecimal64());

        for (int row = 0; row < batch.size; row++) {
            ColumnVector vector = batch.cols[COLUMN_ID];
            int idx = row;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                // check null
                Assert.assertTrue(block.isNull(row));
            } else {
                Assert.assertTrue(vector instanceof BytesColumnVector);
                BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;

                int pos = bytesColumnVector.start[idx];
                int len = bytesColumnVector.length[idx];
                byte[] tmp = new byte[len];

                boolean isUtf8FromLatin1 =
                    MySQLUnicodeUtils.utf8ToLatin1(bytesColumnVector.vector[idx], pos, pos + len, tmp);
                if (!isUtf8FromLatin1) {
                    // in columnar, decimals are stored already in latin1 encoding
                    System.arraycopy(bytesColumnVector.vector[idx], pos, tmp, 0, len);
                }
                DecimalStructure targetDec = new DecimalStructure();
                DecimalConverter.binToDecimal(tmp, targetDec, inputTypes.get(targetColumnId - 1).getPrecision(),
                    inputTypes.get(
                        targetColumnId - 1).getScale());
                Assert.assertEquals(0,
                    FastDecimalUtils.compare(targetDec, block.getDecimal(row).getDecimalStructure()));
            }
        }
    }
}
