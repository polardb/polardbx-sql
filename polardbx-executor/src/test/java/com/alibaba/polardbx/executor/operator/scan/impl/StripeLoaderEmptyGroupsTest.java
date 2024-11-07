package com.alibaba.polardbx.executor.operator.scan.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.StreamName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class StripeLoaderEmptyGroupsTest extends ColumnarStartAtTestBase {

    public StripeLoaderEmptyGroupsTest() throws IOException {
        super("StripeLoaderEmptyGroupsTest.orc", 1, 0);
    }

    @Before
    public void prepare() throws IOException {
        prepareWriting();
        prepareReading();
    }

    private void prepareWriting() throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createDouble());

        int rowCount = 31500;

        try (Writer writer = OrcFile.createWriter(filePath,
            OrcFile.writerOptions(configuration)
                .fileSystem(fileSystem)
                .overwrite(true)
                .rowIndexStride(DEFAULT_INDEX_STRIDE)
                .setSchema(schema))) {

            VectorizedRowBatch batch = schema.createRowBatch(1000);

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                // Populate the rowIdx
                ((DoubleColumnVector) batch.cols[0]).vector[batch.size] = rowIndex + 0.5d;

                batch.size += 1;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
    }

    @Test
    public void testColumnReader() throws IOException {
        DoubleColumnReader doubleColumnReader = new DoubleColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, indexStride, false
        );

        Arrays.fill(rowGroupIncluded, false);
        doubleColumnReader.open(true, rowGroupIncluded);

        // this columnar reader is not accessible.
    }

    @Test
    public void testColumnLoading1() throws ExecutionException, InterruptedException {
        Arrays.fill(rowGroupIncluded, false);
        CompletableFuture<Map<StreamName, InStream>> loadFuture =
            stripeLoader.load(ImmutableList.of(columnId), ImmutableMap.of(columnId, rowGroupIncluded));
        Map<StreamName, InStream> result = loadFuture.get();
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testColumnLoading2() throws ExecutionException, InterruptedException {
        Arrays.fill(rowGroupIncluded, false);
        CompletableFuture<Map<StreamName, InStream>> loadFuture =
            stripeLoader.load(columnId, rowGroupIncluded);
        Map<StreamName, InStream> result = loadFuture.get();
        Assert.assertTrue(result.isEmpty());
    }
}