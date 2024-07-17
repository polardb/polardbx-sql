package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
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
 * binary type
 */
public abstract class BinaryTestBase extends ColumnTestBase {

    protected static final String TEST_ORC_FILE_NAME = "binary_type.orc";

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
        // binary (without dictionary)
        // binary (with dictionary)
        return ImmutableList.of(
            DataTypes.IntegerType,
            DataTypes.BinaryType,
            DataTypes.BinaryType
        );
    }

    @Override
    protected String getOrcFilename() {
        return TEST_ORC_FILE_NAME;
    }
}
