package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.core.MySQLTimeVal;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static com.alibaba.polardbx.rpc.result.XResultUtil.ZERO_TIMESTAMP_LONG_VAL;

public class TimestampColumnReaderTest extends FullTypeScanTestBase {

    private static final int COLUMN_ID = 7;

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
        Assert.assertTrue(block instanceof TimestampBlock);

        for (int row = 0; row < batch.size; row++) {
            ColumnVector vector = batch.cols[COLUMN_ID];

            if (vector.isNull[row]) {
                // check null
                Assert.assertTrue(block.isNull(row));
            } else {
                Assert.assertTrue(vector instanceof LongColumnVector);
                long[] array = ((LongColumnVector) vector).vector;
                int idx = row;
                if (vector.isRepeating) {
                    idx = 0;
                }
                MysqlDateTime mysqlDateTime;
                if (array[idx] == ZERO_TIMESTAMP_LONG_VAL) {
                    mysqlDateTime = MysqlDateTime.zeroDateTime();
                } else {
                    MySQLTimeVal mySQLTimeVal = XResultUtil.longToTimeValue(array[idx]);
                    mysqlDateTime = MySQLTimeConverter.convertTimestampToDatetime(mySQLTimeVal,
                        ((TimestampBlock) block).getTimezone().toZoneId());
                }
                OriginalTimestamp originalTimestamp = new OriginalTimestamp(mysqlDateTime);
                Assert.assertEquals(originalTimestamp, block.getTimestamp(row));
            }
        }
    }
}
