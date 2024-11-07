package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
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
import java.util.stream.Collectors;

public class EnumColumnNonAsciiTest extends ColumnarStartAtTestBase {
    private Map<String, Integer> enumValues = new HashMap<>();

    public EnumColumnNonAsciiTest() throws IOException {
        super("direct_enum_test.orc", 1, 0);
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

                ((BytesColumnVector) batch.cols[0]).setVal(batch.size, ("枚举" + rowIndex).getBytes());

                enumValues.put("枚举" + rowIndex, rowIndex);

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

        DirectEnumColumnReader directEnumColumnReader = new DirectEnumColumnReader(
            columnId, false,
            stripeLoader,
            orcIndex,
            null, indexStride, false, new EnumType(enumValues.keySet().stream().collect(Collectors.toList()))
        );

        directEnumColumnReader.open(true, rowGroupIncluded);

        // Read position 1005, and value should be "枚举1005"
        directEnumColumnReader.startAt(0, 1000);
        EnumBlock block = new EnumBlock(1000, enumValues);
        directEnumColumnReader.next(block, 1000);
        RandomAccessBlock result = BlockUtils.fillSelection(block, new int[] {5}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "枚举1005", result.elementAt(0));

        // Read position 2010, and value should be "枚举2010"
        directEnumColumnReader.startAt(0, 2000);
        block = new EnumBlock(1000, enumValues);
        directEnumColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {10}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "枚举2010", result.elementAt(0));

        // Read position 7333, and value should be "枚举7333"
        directEnumColumnReader.startAt(0, 7000);
        block = new EnumBlock(1000, enumValues);
        directEnumColumnReader.next(block, 1000);
        result = BlockUtils.fillSelection(block, new int[] {333}, 1, false, false, null);
        Assert.assertEquals("actual = " + result.elementAt(0), "枚举7333", result.elementAt(0));
    }
}