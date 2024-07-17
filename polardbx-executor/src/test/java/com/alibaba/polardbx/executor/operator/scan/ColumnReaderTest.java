package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.LongColumnReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.StreamName;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Use apache-orc SDK RecordReader to check the validity of
 * ColumnReader and StripeLoader.
 */
public class ColumnReaderTest extends ScanTestBase {

    public static final String METRICS_NAME = new StringBuilder()
        .append("ScanWork$")
        .append("1d74bd93f993224").append('$')
        .append(943268).append('$')
        .append(0).toString();

    @Test
    public void testSingleGroup() throws IOException {
        doTest(0, 2,
            new boolean[] {true, true, true, true, true},
            fromRowGroupIds(0, new int[] {0}),
            ImmutableList.of(
                new BlockLocation(0, 0, 1000),
                new BlockLocation(0, 1000, 1000),
                new BlockLocation(0, 2000, 1000)
            ));

//        doPrint(stripeId, orcTail.getStripes(), locationList, columnId);
    }

    @Test
    public void testMultiGroup() throws IOException {
        doTest(0, 2,
            new boolean[] {true, true, true, true, true},
            fromRowGroupIds(0, new int[] {0, 1, 2}),
            ImmutableList.of(
                new BlockLocation(0, 0, 1000),
                new BlockLocation(1, 1000, 1000),
                new BlockLocation(2, 2000, 1000)
            )
        );
    }

    @Test
    public void testWithoutPresent() throws IOException {
        final int stripeId = 0;
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);
        // pk column without present stream.
        final int columnId = 1;
        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] columnIncluded = {true, true, true, true, true};
        final boolean[] rowGroupIncluded = new boolean[groupsInStripe];
        Arrays.fill(rowGroupIncluded, false);
        rowGroupIncluded[0] = true;

        final OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );

        // metrics named ColumnReaderTest
        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create(METRICS_NAME);
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

            // System.out.println(runtimeMetrics.reportAll());
            columnReader = new LongColumnReader(columnId, true, stripeLoader, orcIndex, runtimeMetrics,
                OrcProto.ColumnEncoding.Kind.DIRECT_V2, indexStride, true);

            columnReader.open(loadFuture, false, rowGroupIncluded);

            int groupId = 0;
            int rowCountInGroup = getRowCount(stripeInformation, groupId);
            int chunkLimit = 1000;
            int startPosition = 0;

            int blockCount = 0;
            int totalRows = 0;
            for (; startPosition < rowCountInGroup; startPosition += chunkLimit) {
                int positionCount = Math.min(chunkLimit, rowCountInGroup - startPosition);
                Block block = new LongBlock(DataTypes.LongType, positionCount);

                // The seeking may be blocked util IO processing completed.
                columnReader.startAt(groupId, startPosition);
                try {
                    columnReader.next((RandomAccessBlock) block, positionCount);

                    blockCount++;
                    totalRows += block.getPositionCount();

                    // print 1000_000 ~ 1000_000 + 9999
                    // print(block);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            Assert.assertEquals(
                (rowCountInGroup + chunkLimit - 1) / chunkLimit, blockCount);

            Assert.assertEquals(totalRows, rowCountInGroup);

        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }

            if (columnReader != null) {
                columnReader.close();
            }
        }
    }

    @Test
    public void testWithPresent() throws IOException {
        // 1000000, -5714940555797352449, "ygdirym8is433oxu", null,
        // 1000001, 2058245259437398298, "qc3hmfhkusu8zxdq", "vbq6bc5npru08v0ob",
        // 1000002, -1637472836889869553, null, "2l7v91bh5jocns9",
        // 1000003, 9129995099457418016, null, null,
        // 1000004, null, "8s0q9p83333c0neo", "89jxhhds6rddns9io",
        // 1000005, -5331604852030977563, "2pxidqjvyq2us1i", "1hldduxyevxrbrx",
        // 1000006, 2748973064230422743, "2rrha0ayg7p8hrn", "xdalc4xkohor2bxqs",
        // 1000007, 4919203704928141213, "qfqbe6cdq1cynqq", "k2xh2w697ily4fo6a",
        // 1000008, 701980881788458824, "kumbszyftgxd34as", "c85eqjvi1d5d5fq",
        // 1000009, 4293562962769452696, "9bhnbg3orr7bkej", "8fyk9ebn5pp9y32",

        final int stripeId = 0;
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);
        // pk column without present stream.
        final int columnId = 2;
        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] columnIncluded = {true, true, true, true, true};
        final boolean[] rowGroupIncluded = new boolean[groupsInStripe];
        Arrays.fill(rowGroupIncluded, false);
        rowGroupIncluded[0] = true;

        final OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );

        // metrics named ColumnReaderTest
        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create(METRICS_NAME);
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

            // System.out.println(runtimeMetrics.reportAll());
            columnReader = new LongColumnReader(columnId, true, stripeLoader, orcIndex, runtimeMetrics,
                OrcProto.ColumnEncoding.Kind.DIRECT_V2, indexStride, true);
            columnReader.open(loadFuture, false, rowGroupIncluded);

            int groupId = 0;
            int rowCountInGroup = getRowCount(stripeInformation, groupId);
            int chunkLimit = 1000;
            int startPosition = 0;

            int blockCount = 0;
            int totalRows = 0;
            for (; startPosition < rowCountInGroup; startPosition += chunkLimit) {
                int positionCount = Math.min(chunkLimit, rowCountInGroup - startPosition);
                Block block = new LongBlock(DataTypes.LongType, positionCount);

                // The seeking may be blocked util IO processing completed.
                columnReader.startAt(groupId, startPosition);
                try {
                    columnReader.next((RandomAccessBlock) block, positionCount);

                    blockCount++;
                    totalRows += block.getPositionCount();

                    // print 1000_000 ~ 1000_000 + 9999
                    if (startPosition >= 0) {
                        // print(block);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            Assert.assertEquals(
                (rowCountInGroup + chunkLimit - 1) / chunkLimit, blockCount);

            Assert.assertEquals(totalRows, rowCountInGroup);

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
        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create(METRICS_NAME);
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
            columnReader = new LongColumnReader(columnId, true, stripeLoader, orcIndex, runtimeMetrics,
                OrcProto.ColumnEncoding.Kind.DIRECT_V2, indexStride, true);
            columnReader.open(loadFuture, false, rowGroupIncluded);

            for (BlockLocation location : locationList) {
                // block builder matched with raw orc data.
                Block block = new LongBlock(DataTypes.LongType, location.positionCount);

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

    private void doPrint(int stripeId,
                         List<StripeInformation> stripeInformationList,
                         List<BlockLocation> blockLocationList,
                         int targetColumnId) throws IOException {
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

            for (BlockLocation location : blockLocationList) {

                // count start row position in given row group.
                int startPositionInGroup = stripeStartRows + location.rowGroupId * indexStride;
                rows.seekToRow(startPositionInGroup + location.startPosition);

                // seek and read values with position count.
                VectorizedRowBatch batch = SCHEMA.createRowBatch(location.positionCount);
                rows.nextBatch(batch);

                print(batch, targetColumnId);
            }

        }
    }

    protected void print(Block block) {
        for (int i = 0; i < block.getPositionCount(); i++) {
            System.out.println(block.getObject(i));
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
                    Assert.assertEquals(String.valueOf(block.getObject(row)), builder.toString());
                    // System.out.println(block.getObject(row) + ", ");
                }
            }

        }
    }

    private static void print(VectorizedRowBatch batch, int targetColumnId) {
        for (int row = 0; row < batch.size; row++) {
            StringBuilder builder = new StringBuilder();

            for (int columnIndex = 0; columnIndex < batch.cols.length; columnIndex++) {
                if (targetColumnId != columnIndex + 1) {
                    continue;
                }

                ColumnVector vector = batch.cols[columnIndex];

                if (vector.isNull[row]) {
                    builder.append("null, ");
                } else {
                    vector.stringifyValue(builder, row);
                    builder.append(", ");
                }
            }
            System.out.println(builder);
        }
    }

}
