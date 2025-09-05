package com.alibaba.polardbx.qatest.columnar.ddl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

public class ColumnarTtl20Test extends ColumnarDdlAutoLoadSqlTestBase {

    public ColumnarTtl20Test(PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams params) {
        super(params);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<PartitionAutoLoadSqlTestBase.AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(ColumnarTtl20Test.class, 3, true);
    }
}
