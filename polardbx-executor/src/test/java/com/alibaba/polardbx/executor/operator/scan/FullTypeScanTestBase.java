package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Contains types which are not covered in TPC-H tables
 */
public abstract class FullTypeScanTestBase extends ColumnTestBase {

    protected static final String TEST_ORC_FILE_NAME = "full_type.orc";

    @BeforeClass
    public static void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 1000);
        FILE_PATH = new Path(getFileFromClasspath(TEST_ORC_FILE_NAME));

        FILESYSTEM = FileSystem.get(
            FILE_PATH.toUri(), CONFIGURATION
        );
    }

    @Override
    protected List<DataType> createInputTypes() {
        // int(id)
        // smallint
        // mediumint
        // tiny
        // float
        // double
        // year
        // timestamp
        // json
        // normal decimal(20,2)
        // bit
        // blob
        return ImmutableList.of(
            DataTypes.IntegerType,
            DataTypes.ShortType,
            DataTypes.MediumIntType,
            DataTypes.TinyIntType,
            DataTypes.FloatType,
            DataTypes.DoubleType,
            DataTypes.YearType,
            DataTypes.TimestampType,
            DataTypes.JsonType,
            new DecimalType(20, 2),
            DataTypes.BitType,
            DataTypes.BlobType
        );
    }

    @Override
    protected String getOrcFilename() {
        return TEST_ORC_FILE_NAME;
    }
}
