package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.StreamName;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

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

    @Override
    protected void doTest(int stripeId, int columnId,
                          boolean[] columnIncluded,
                          boolean[] rowGroupIncluded,
                          List<BlockLocation> locationList)
        throws IOException {

        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );
        final OrcProto.ColumnEncoding[] encodings = encodingMap.get(stripeId);

        // metrics named ColumnReaderTest
        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("Test");
        // Add parent metrics node
        runtimeMetrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_BYTES_RANGE,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        runtimeMetrics.addDerivedCounter(ColumnReader.COLUMN_READER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(ColumnReader.COLUMN_READER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);

        runtimeMetrics.addDerivedCounter(LogicalRowGroup.BLOCK_LOAD_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(LogicalRowGroup.BLOCK_MEMORY_COUNTER,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        StripeLoader stripeLoader = null;
        ColumnReader columnReader = null;
        try {
            stripeLoader = createStripeLoader(stripeId, columnIncluded, runtimeMetrics);
            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(columnId, rowGroupIncluded);

            // Use async mode and wait for completion of stripe loader.
            columnReader = createColumnReader(columnId, orcIndex, runtimeMetrics, stripeLoader, encodings);
            columnReader.open(loadFuture, false, rowGroupIncluded);

            for (ScanTestBase.BlockLocation location : locationList) {
                // block builder matched with raw orc data.
                Block block = allocateBlock(columnReader, inputTypes.get(columnId - 1), false,
                    TimeZone.getDefault(), encodings[columnId], location.positionCount);

                // move index of column-reader and start reading from this index.
                columnReader.startAt(location.rowGroupId, location.startPosition);
                columnReader.next((RandomAccessBlock) block, location.positionCount);

                Slice slice = ((SliceBlock) block).getData();
                int bytesSize = ((byte[]) slice.getBase()).length;
                int sliceSize = slice.length();
                System.out.println("bytesSize = " + bytesSize + ", sliceSize = " + sliceSize);
                Assert.assertTrue(bytesSize == sliceSize);

                // Check block
                doValidate(block, columnId, stripeId, orcTail.getStripes(), location);
            }

            System.out.println(runtimeMetrics.reportAll());
        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
            if (columnReader != null) {
                columnReader.close();
            }
        }
    }
}
