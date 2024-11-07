package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnumColumnDictionaryNonAsciiTest extends ColumnarStartAtTestBase {
    private Map<String, Integer> enumValues = new HashMap<>();
    private final String[] enumStrings = {"枚举值1", "枚举值2", "枚举值3", "枚举值4", "枚举值5", "枚举值6",};

    public EnumColumnDictionaryNonAsciiTest() throws IOException {
        super("dictionary_enum_test.orc", 1, 0);
    }

    @Before
    public void prepare() throws IOException {
        prepareWriting();
        prepareReading();
        for (int i = 0; i < enumStrings.length; i++) {
            enumValues.put(enumStrings[i], i);
        }
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

                ((BytesColumnVector) batch.cols[0]).setVal(batch.size,
                    (enumStrings[rowIndex % enumStrings.length]).getBytes());

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

        DictionaryEnumColumnReader directEnumColumnReader = new DictionaryEnumColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, encodings[columnId], indexStride, false
        );

        directEnumColumnReader.open(true, rowGroupIncluded);

        // Read position 1005, and value should be "枚举1005"
        directEnumColumnReader.startAt(0, 1000);
        EnumBlock block = new EnumBlock(1000, enumValues);
        directEnumColumnReader.next(block, 1000);
        RandomAccessBlock result = BlockUtils.fillSelection(block, new int[] {5}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), enumStrings[1005 % enumStrings.length],
            result.elementAt(0));

        // Read position 2010, and value should be "枚举2010"
        directEnumColumnReader.startAt(0, 2000);
        block = new EnumBlock(1000, enumValues);
        directEnumColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {10}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), enumStrings[2010 % enumStrings.length],
            result.elementAt(0));

        // Read position 7333, and value should be "枚举7333"
        directEnumColumnReader.startAt(0, 7000);
        block = new EnumBlock(1000, enumValues);
        directEnumColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {333}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), enumStrings[7333 % enumStrings.length],
            result.elementAt(0));
    }
}