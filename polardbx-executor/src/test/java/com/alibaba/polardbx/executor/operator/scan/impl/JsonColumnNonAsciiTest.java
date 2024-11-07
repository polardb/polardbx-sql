package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.JsonType;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class JsonColumnNonAsciiTest extends ColumnarStartAtTestBase {
    public JsonColumnNonAsciiTest() throws IOException {
        super("direct_json_test.orc", 1, 0);
    }

    @Before
    public void prepare() throws IOException {
        prepareWriting();
        prepareReading();
    }

    private void prepareWriting() throws IOException {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("f1", TypeDescription.createString());

        int rowCount = 31500;

        try (Writer writer = OrcFile.createWriter(filePath,
            OrcFile.writerOptions(configuration)
                .fileSystem(fileSystem)
                .overwrite(true)
                .rowIndexStride(DEFAULT_INDEX_STRIDE)
                .setSchema(schema))) {

            VectorizedRowBatch batch = schema.createRowBatch(1000);

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {

                ((BytesColumnVector) batch.cols[0]).setVal(batch.size, ("中文" + rowIndex).getBytes());

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

        DataType inputType = new JsonType();

        DirectJsonColumnReader directJsonColumnReader = new DirectJsonColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, indexStride, false, new JsonType()
        );

        directJsonColumnReader.open(true, rowGroupIncluded);

        // Read position 1005, and value should be "中文1005"
        directJsonColumnReader.startAt(0, 1000);
        StringBlock block = new StringBlock(inputType, 1000);
        directJsonColumnReader.next(block, 1000);
        RandomAccessBlock result = BlockUtils.fillSelection(block, new int[] {5}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "中文1005", result.elementAt(0));

        // Read position 2010, and value should be "中文2010"
        directJsonColumnReader.startAt(0, 2000);
        block = new StringBlock(inputType, 1000);
        directJsonColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {10}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "中文2010", result.elementAt(0));

        // Read position 7333, and value should be "中文7333"
        directJsonColumnReader.startAt(0, 7000);
        block = new StringBlock(inputType, 1000);
        directJsonColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {333}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "中文7333", result.elementAt(0));
    }
}
