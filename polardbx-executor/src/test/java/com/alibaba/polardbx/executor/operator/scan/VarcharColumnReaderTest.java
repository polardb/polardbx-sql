package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.DirectVarcharColumnReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.StreamName;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class VarcharColumnReaderTest extends ScanTestBase {
    @Test
    public void test() throws IOException {
        doTest(
            0, 3,
            new boolean[] {true, true, true, true, false},
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new BlockLocation(0, 0, 1000),
                new BlockLocation(1, 1000, 1000),
                new BlockLocation(2, 2000, 1000),
                new BlockLocation(2, 4000, 1000)
            )
        );
    }

    public void doTest(int stripeId, int columnId,
                       boolean[] columnIncluded,
                       boolean[] rowGroupIncluded,
                       List<BlockLocation> locationList)
        throws IOException {
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final ExecutionContext context = new ExecutionContext();
        final OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );

        // metrics named ColumnReaderTest
        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("VarcharColumnReaderTest");
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
            columnReader =
                new DirectVarcharColumnReader(columnId, true, stripeLoader, orcIndex, runtimeMetrics, indexStride,
                    true);
            columnReader.open(loadFuture, false, rowGroupIncluded);

            for (BlockLocation location : locationList) {
                // block builder matched with raw orc data.
                Block block = new SliceBlock(new SliceType(), location.positionCount, false, false);

                // move index of column-reader and start reading from this index.
                columnReader.startAt(location.rowGroupId, location.startPosition);
                columnReader.next((RandomAccessBlock) block, location.positionCount);

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

    private void doValidate(
        Block targetBlock,
        int targetColumnId,
        int stripeId,
        List<StripeInformation> stripeInformationList,
        BlockLocation location) throws IOException {
        StripeInformation stripeInformation = stripeInformationList.get(stripeId);

        // count the total rows before this stripe.
        int stripeStartRows = 0;
        for (int i = 0; i < stripeId; i++) {
            stripeStartRows += stripeInformationList.get(i).getNumberOfRows();
        }

        Path path = new Path(getFileFromClasspath
            (TEST_ORC_FILE_NAME));

        Reader.Options options = new Reader.Options(CONFIGURATION).schema(SCHEMA)
            .range(stripeInformation.getOffset(), stripeInformation.getLength());

        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(CONFIGURATION));
            RecordReader rows = reader.rows(options)) {

            // count start row position in given row group.
            int startPositionInGroup = stripeStartRows + location.rowGroupId * indexStride;
            rows.seekToRow(startPositionInGroup + location.startPosition);

            // seek and read values with position count.
            VectorizedRowBatch batch = SCHEMA.createRowBatch(location.positionCount);
            rows.nextBatch(batch);

            check(targetBlock, batch, targetColumnId);
        }
    }

    private static void check(Block block, VectorizedRowBatch batch, int targetColumnId) {
        for (int row = 0; row < batch.size; row++) {
            for (int columnIndex = 0; columnIndex < batch.cols.length; columnIndex++) {
                if (targetColumnId != columnIndex + 1) {
                    continue;
                }

                ColumnVector vector = batch.cols[columnIndex];
                if (vector.isNull[row]) {
                    // check null
                    Assert.assertTrue(block.isNull(row));
                    // System.out.println("null,");
                } else {
                    // check non null
                    StringBuilder builder = new StringBuilder();
                    vector.stringifyValue(builder, row);
                    Assert.assertEquals(
                        "\"" + ((Slice) block.getObject(row)).toStringUtf8() + "\"",
                        builder.toString());
                    // System.out.println(block.getObject(row) + ", ");
                }
            }

        }
    }
}
