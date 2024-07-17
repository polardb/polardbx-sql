package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static com.alibaba.polardbx.executor.operator.scan.ScanTestBase.BlockLocation;

public class DictionaryColumnReaderTest extends TpchColumnTestBase {
    // dictionary encoding column:
    // colId = 9,10,14,15,16

    // l_returnflag
    @Test
    public void testDict1() throws IOException {
        final int columnId = 9;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new BlockLocation(0, 0, 100),
                new BlockLocation(1, 100, 100),
                new BlockLocation(2, 200, 100),
                new BlockLocation(2, 400, 100)
            )
        );
    }

    @Test
    public void testDict1DisableSliceDict() throws IOException {
        context.getParamManager().getProps().put(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT, "false");

        final int columnId = 9;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new BlockLocation(0, 0, 100),
                new BlockLocation(1, 100, 100),
                new BlockLocation(2, 200, 100),
                new BlockLocation(2, 400, 100)
            )
        );
    }

    // l_linestatus
    @Test
    public void testDict2() throws IOException {
        final int columnId = 10;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new BlockLocation(0, 0, 100),
                new BlockLocation(1, 100, 100),
                new BlockLocation(1, 200, 100),
                new BlockLocation(1, 300, 100),
                new BlockLocation(1, 400, 100)
            )
        );
    }

    @Test
    public void testDict2DisableSliceDict() throws IOException {
        context.getParamManager().getProps().put(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT, "false");

        final int columnId = 10;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new BlockLocation(0, 0, 100),
                new BlockLocation(1, 100, 100),
                new BlockLocation(1, 200, 100),
                new BlockLocation(1, 300, 100),
                new BlockLocation(1, 400, 100)
            )
        );
    }

    // l_shipmode
    @Test
    public void testDict3DisableSliceDict() throws IOException {
        context.getParamManager().getProps().put(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT, "false");

        final int columnId = 15;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 5}),
            ImmutableList.of(
                new BlockLocation(0, 500, 100),
                new BlockLocation(1, 500, 100),
                new BlockLocation(2, 500, 100),
                new BlockLocation(3, 300, 100),
                new BlockLocation(4, 400, 100)
            )
        );
    }

    @Test
    public void testDict3() throws IOException {
        final int columnId = 15;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 5}),
            ImmutableList.of(
                new BlockLocation(0, 500, 100),
                new BlockLocation(1, 500, 100),
                new BlockLocation(2, 500, 100),
                new BlockLocation(3, 300, 100),
                new BlockLocation(4, 400, 100)
            )
        );
    }

    // l_comment
    @Test
    public void testDict4() throws IOException {
        final int columnId = 16;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 5, 6}),
            ImmutableList.of(
                new BlockLocation(0, 500, 100),
                new BlockLocation(1, 500, 100),
                new BlockLocation(2, 500, 100),
                new BlockLocation(3, 300, 100),
                new BlockLocation(4, 400, 100),
                new BlockLocation(0, 200, 100),
                new BlockLocation(1, 200, 100),
                new BlockLocation(2, 200, 100),
                new BlockLocation(3, 200, 100),
                new BlockLocation(4, 200, 100)
            )
        );
    }

    // l_comment
    @Test
    public void testDict4DisableSliceDict() throws IOException {
        context.getParamManager().getProps().put(ConnectionProperties.ENABLE_COLUMNAR_SLICE_DICT, "false");

        final int columnId = 16;
        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, false);
        columnIncluded[0] = true;
        columnIncluded[columnId] = true;

        doTest(
            0, columnId,
            columnIncluded,
            fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 5, 6}),
            ImmutableList.of(
                new BlockLocation(0, 500, 100),
                new BlockLocation(1, 500, 100),
                new BlockLocation(2, 500, 100),
                new BlockLocation(3, 300, 100),
                new BlockLocation(4, 400, 100),
                new BlockLocation(0, 200, 100),
                new BlockLocation(1, 200, 100),
                new BlockLocation(2, 200, 100),
                new BlockLocation(3, 200, 100),
                new BlockLocation(4, 200, 100)
            )
        );
    }
}
