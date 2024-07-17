package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Contains TPC-H required types
 */
public class TpchColumnTestBase extends ColumnTestBase {
    protected static final String TEST_ORC_FILE_NAME = "57cb28ac0ba7.orc";

    @BeforeClass
    public static void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();

        FILE_PATH = new Path(getFileFromClasspath(TEST_ORC_FILE_NAME));

        FILESYSTEM = FileSystem.get(FILE_PATH.toUri(), CONFIGURATION);
    }

    @Override
    protected String getOrcFilename() {
        return TEST_ORC_FILE_NAME;
    }

    // for comparison.
    @Test
    @Ignore
    public void testOrc() throws IOException {
        Path path = new Path(getFileFromClasspath(getOrcFilename()));

        // dictionary encoding column:
        // colId = 9,10,14,15,16
        final int colId = 11;
        TypeDescription schema = TypeDescription.createStruct()
            .addField(fileSchema.getFieldNames().get(colId - 1), fileSchema.getChildren().get(colId - 1));

        Reader.Options options = new Reader.Options(CONFIGURATION).schema(schema);
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(CONFIGURATION));
            RecordReader rows = reader.rows(options)) {

            VectorizedRowBatch batch = schema.createRowBatch(DEFAULT_CHUNK_LIMIT);

            rows.nextBatch(batch);

            print(batch);
        }
    }

    @Override
    protected void check(Block block, VectorizedRowBatch batch, int targetColumnId) {
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

                    if (block instanceof SliceBlock) {
                        boolean enableSliceDict =
                            context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SLICE_DICT);
                        if (!enableSliceDict) {
                            Assert.assertNull(((SliceBlock) block).getDictionary());
                        }
                        Assert.assertEquals(
                            "\"" + ((Slice) block.getObject(row)).toStringUtf8() + "\"",
                            builder.toString());
                    } else if (block instanceof DateBlock) {
                        Assert.assertEquals(
                            String.valueOf(((DateBlock) block).getPacked()[row]), builder.toString());
                    } else {
                        throw GeneralUtil.nestedException("Add your assertion");
                    }

                    // System.out.println(block.getObject(row) + ", ");
                }
            }

        }
    }

    @Override
    protected List<DataType> createInputTypes() {
        //	`l_orderkey` int(11) NOT NULL,
        //	`l_partkey` int(11) NOT NULL,
        //	`l_suppkey` int(11) NOT NULL,
        //	`l_linenumber` int(11) NOT NULL,
        //	`l_quantity` decimal(15, 2) NOT NULL,
        //	`l_extendedprice` decimal(15, 2) NOT NULL,
        //	`l_discount` decimal(15, 2) NOT NULL,
        //	`l_tax` decimal(15, 2) NOT NULL,
        //	`l_returnflag` varchar(1) NOT NULL,
        //	`l_linestatus` varchar(1) NOT NULL,
        //	`l_shipdate` date NOT NULL,
        //	`l_commitdate` date NOT NULL,
        //	`l_receiptdate` date NOT NULL,
        //	`l_shipinstruct` varchar(25) NOT NULL,
        //	`l_shipmode` varchar(10) NOT NULL,
        //	`l_comment` varchar(44) NOT NULL,
        return ImmutableList.of(
            DataTypes.IntegerType,
            DataTypes.IntegerType,
            DataTypes.IntegerType,
            DataTypes.IntegerType,
            new DecimalType(15, 2),
            new DecimalType(15, 2),
            new DecimalType(15, 2),
            new DecimalType(15, 2),
            new VarcharType(1),
            new VarcharType(1),
            new DateType(),
            new DateType(),
            new DateType(),
            new VarcharType(25),
            new VarcharType(10),
            new VarcharType(44)
        );
    }
}
