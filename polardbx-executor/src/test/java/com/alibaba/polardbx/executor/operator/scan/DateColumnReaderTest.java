package com.alibaba.polardbx.executor.operator.scan;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

////	`l_orderkey` int(11) NOT NULL,
////	`l_partkey` int(11) NOT NULL,
////	`l_suppkey` int(11) NOT NULL,
////	`l_linenumber` int(11) NOT NULL,
////	`l_quantity` decimal(15, 2) NOT NULL,
////	`l_extendedprice` decimal(15, 2) NOT NULL,
////	`l_discount` decimal(15, 2) NOT NULL,
////	`l_tax` decimal(15, 2) NOT NULL,
////	`l_returnflag` varchar(1) NOT NULL,
////	`l_linestatus` varchar(1) NOT NULL,
////	`l_shipdate` date NOT NULL,
////	`l_commitdate` date NOT NULL,
////	`l_receiptdate` date NOT NULL,
////	`l_shipinstruct` varchar(25) NOT NULL,
////	`l_shipmode` varchar(10) NOT NULL,
////	`l_comment` varchar(44) NOT NULL,
public class DateColumnReaderTest extends TpchColumnTestBase {
    // date: colId = 11,12,13

    // l_shipdate
    @Test
    public void test1() throws IOException {
        final int columnId = 11;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new ScanTestBase.BlockLocation(0, 0, 100),
                new ScanTestBase.BlockLocation(1, 100, 100),
                new ScanTestBase.BlockLocation(2, 200, 100),
                new ScanTestBase.BlockLocation(2, 400, 100)
            )
        );
    }

    // l_commitdate
    @Test
    public void testDict3() throws IOException {
        final int columnId = 12;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 5}),
            ImmutableList.of(
                new ScanTestBase.BlockLocation(0, 500, 100),
                new ScanTestBase.BlockLocation(1, 500, 100),
                new ScanTestBase.BlockLocation(2, 500, 100),
                new ScanTestBase.BlockLocation(3, 300, 100),
                new ScanTestBase.BlockLocation(4, 400, 100),
                new ScanTestBase.BlockLocation(0, 700, 100),
                new ScanTestBase.BlockLocation(1, 700, 100),
                new ScanTestBase.BlockLocation(2, 700, 100),
                new ScanTestBase.BlockLocation(3, 700, 100),
                new ScanTestBase.BlockLocation(4, 100, 100)
            )
        );
    }

    // l_receiptdate
    @Test
    public void testDict4() throws IOException {
        final int columnId = 13;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 5, 6}),
            ImmutableList.of(
                new ScanTestBase.BlockLocation(0, 500, 100),
                new ScanTestBase.BlockLocation(1, 500, 100),
                new ScanTestBase.BlockLocation(2, 500, 100),
                new ScanTestBase.BlockLocation(3, 300, 100),
                new ScanTestBase.BlockLocation(4, 400, 100),
                new ScanTestBase.BlockLocation(0, 200, 100),
                new ScanTestBase.BlockLocation(1, 200, 100),
                new ScanTestBase.BlockLocation(2, 200, 100),
                new ScanTestBase.BlockLocation(3, 200, 100),
                new ScanTestBase.BlockLocation(4, 200, 100),
                new ScanTestBase.BlockLocation(1, 700, 100),
                new ScanTestBase.BlockLocation(2, 700, 100),
                new ScanTestBase.BlockLocation(3, 700, 100),
                new ScanTestBase.BlockLocation(4, 700, 100)
            )
        );
    }
}
