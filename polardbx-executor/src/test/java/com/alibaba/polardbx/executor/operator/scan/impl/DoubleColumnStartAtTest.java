package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class DoubleColumnStartAtTest extends ColumnarStartAtTestBase {

    public DoubleColumnStartAtTest() throws IOException {
        super("double_test.orc", 1, 0);
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
    public void test() throws IOException {
        DoubleColumnReader doubleColumnReader = new DoubleColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, indexStride, false
        );

        doubleColumnReader.open(true, rowGroupIncluded);

        // Read position 1005, and value should be 1005.5
        doubleColumnReader.startAt(0, 1000);
        DoubleBlock block = new DoubleBlock(DataTypes.DoubleType, 1000);
        doubleColumnReader.next(block, 1000);
        RandomAccessBlock result = BlockUtils.fillSelection(block, new int[] {5}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), 1005.5d, result.elementAt(0));

        // Read position 2010, and value should be 2010.5
        doubleColumnReader.startAt(0, 2000);
        block = new DoubleBlock(DataTypes.DoubleType, 1000);
        doubleColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {10}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), 2010.5, result.elementAt(0));

        // Read position 7333, and value should be 7333.5
        doubleColumnReader.startAt(0, 7000);
        block = new DoubleBlock(DataTypes.DoubleType, 1000);
        doubleColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {333}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), 7333.5, result.elementAt(0));
    }
}
