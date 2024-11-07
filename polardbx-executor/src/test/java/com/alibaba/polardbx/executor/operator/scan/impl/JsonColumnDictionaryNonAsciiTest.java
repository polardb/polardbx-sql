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

public class JsonColumnDictionaryNonAsciiTest extends ColumnarStartAtTestBase {
    public JsonColumnDictionaryNonAsciiTest() throws IOException {
        super("dictionary_json_test.orc", 1, 0);
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

                // low cardinality.
                ((BytesColumnVector) batch.cols[0]).setVal(batch.size, ("中文" + (rowIndex % 100)).getBytes());

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

        DictionaryJsonColumnReader dictionaryJsonColumnReader = new DictionaryJsonColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, encodings[columnId], indexStride, false);

        dictionaryJsonColumnReader.open(true, rowGroupIncluded);

        // Read position 1005, and value should be "中文1005"
        dictionaryJsonColumnReader.startAt(0, 1000);
        StringBlock block = new StringBlock(inputType, 1000);
        dictionaryJsonColumnReader.next(block, 1000);
        RandomAccessBlock result = BlockUtils.fillSelection(block, new int[] {5}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "中文" + (1005 % 100), result.elementAt(0));

        // Read position 2010, and value should be "中文2010"
        dictionaryJsonColumnReader.startAt(0, 2000);
        block = new StringBlock(inputType, 1000);
        dictionaryJsonColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {10}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "中文" + (2010 % 100), result.elementAt(0));

        // Read position 7333, and value should be "中文7333"
        dictionaryJsonColumnReader.startAt(0, 7000);
        block = new StringBlock(inputType, 1000);
        dictionaryJsonColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {333}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "中文" + (7333 % 100), result.elementAt(0));
    }
}
