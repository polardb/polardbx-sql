package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
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

public class FloatColumnStartAtTest extends ColumnarStartAtTestBase {

    public FloatColumnStartAtTest() throws IOException {
        super("float_test.orc", 1, 0);
    }

    @Before
    public void prepare() throws IOException {
        prepareWriting();
        prepareReading();
    }

    private void prepareWriting() throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createFloat());

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
                ((DoubleColumnVector) batch.cols[0]).vector[batch.size] = rowIndex + 0.5f;

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
        FloatColumnReader floatColumnReader = new FloatColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, indexStride, false
        );

        floatColumnReader.open(true, rowGroupIncluded);

        // Read position 1005, and value should be 1005.5
        floatColumnReader.startAt(0, 1000);
        FloatBlock block = new FloatBlock(DataTypes.FloatType, 1000);
        floatColumnReader.next(block, 1000);
        RandomAccessBlock result = BlockUtils.fillSelection(block, new int[] {5}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), 1005.5f, result.elementAt(0));

        // Read position 2010, and value should be 2010.5
        floatColumnReader.startAt(0, 2000);
        block = new FloatBlock(DataTypes.FloatType, 1000);
        floatColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {10}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), 2010.5f, result.elementAt(0));

        // Read position 7333, and value should be 7333.5
        floatColumnReader.startAt(0, 7000);
        block = new FloatBlock(DataTypes.FloatType, 1000);
        floatColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {333}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), 7333.5f, result.elementAt(0));
    }
}
